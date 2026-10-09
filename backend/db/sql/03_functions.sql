-- =============================================================================
-- 03_functions.sql — stored functions used by the ingestion pipeline.
-- Idempotent (CREATE OR REPLACE). Requires PostgreSQL 16+ (pg_input_is_valid).
-- =============================================================================

-- Current date in India; the server itself may run in UTC.
CREATE OR REPLACE FUNCTION market.today_ist()
RETURNS date
LANGUAGE sql STABLE AS $$
    SELECT (now() AT TIME ZONE 'Asia/Kolkata')::date;
$$;

-- -----------------------------------------------------------------------------
-- Calendar
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION market.is_trading_day(p_date date, p_exchange text DEFAULT 'NSE')
RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (SELECT 1 FROM market.special_sessions s
                   WHERE s.exchange = p_exchange AND s.session_date = p_date)
        OR (extract(isodow FROM p_date) < 6
            AND NOT EXISTS (SELECT 1 FROM market.trading_holidays h
                            WHERE h.exchange = p_exchange AND h.holiday_date = p_date));
$$;

CREATE OR REPLACE FUNCTION market.expected_trading_days(p_from date, p_to date, p_exchange text DEFAULT 'NSE')
RETURNS SETOF date
LANGUAGE sql STABLE AS $$
    SELECT d::date
    FROM generate_series(p_from, p_to, interval '1 day') AS g(d)
    WHERE market.is_trading_day(d::date, p_exchange)
    ORDER BY 1;
$$;

-- -----------------------------------------------------------------------------
-- Gap detection
-- Returns contiguous ranges of expected trading days that have neither a stored
-- candle nor a "source has no data" marker. Weekends/holidays inside a range do
-- not split it, so one API request can fill each range.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION market.detect_gaps(p_instrument_id integer, p_from date, p_to date)
RETURNS TABLE (gap_start date, gap_end date, missing_days integer)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_from     date;
    v_exchange text;
BEGIN
    SELECT greatest(p_from, coalesce(i.data_start_date, p_from)), i.exchange
      INTO v_from, v_exchange
      FROM market.instruments i
     WHERE i.instrument_id = p_instrument_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown instrument_id %', p_instrument_id;
    END IF;

    RETURN QUERY
    WITH cal AS (
        SELECT d, row_number() OVER (ORDER BY d) AS rn
        FROM market.expected_trading_days(v_from, p_to, v_exchange) AS d
    ),
    missing AS (
        SELECT c.d, c.rn - row_number() OVER (ORDER BY c.d) AS grp
        FROM cal c
        WHERE NOT EXISTS (SELECT 1 FROM market.ohlcv_daily o
                          WHERE o.instrument_id = p_instrument_id AND o.trade_date = c.d)
          AND NOT EXISTS (SELECT 1 FROM market.no_data_dates n
                          WHERE n.instrument_id = p_instrument_id AND n.trade_date = c.d)
    )
    SELECT min(m.d), max(m.d), count(*)::integer
    FROM missing m
    GROUP BY m.grp
    ORDER BY 1;
END;
$$;

-- -----------------------------------------------------------------------------
-- Batch upsert with row-level validation.
--
-- p_rows is a JSON array of objects:
--   {"trade_date": "2025-01-02", "open": 1.0, "high": 1.0, "low": 1.0,
--    "close": 1.0, "volume": 100, ...any extra raw fields...}
--
-- Every row is type-checked and validated. Invalid rows go to
-- market.ohlcv_quarantine with the reasons; valid rows are upserted into
-- market.ohlcv_daily. Re-running the same batch is a no-op (rows whose values are
-- unchanged are not rewritten), so batches are safe to retry.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION market.upsert_ohlcv_batch(p_run_id bigint, p_instrument_id integer, p_rows jsonb)
RETURNS TABLE (inserted integer, updated integer, unchanged integer, quarantined integer)
LANGUAGE plpgsql AS $$
DECLARE
    v_exchange text;
BEGIN
    SELECT i.exchange INTO v_exchange FROM market.instruments i WHERE i.instrument_id = p_instrument_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown instrument_id %', p_instrument_id;
    END IF;
    IF jsonb_typeof(p_rows) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'p_rows must be a JSON array';
    END IF;

    -- Serialise concurrent writers for the same instrument.
    PERFORM pg_advisory_xact_lock(hashtext('market.ohlcv_daily'), p_instrument_id);

    RETURN QUERY
    WITH src AS (
        SELECT e.val                AS raw,
               e.val ->> 'trade_date' AS d_txt,
               e.val ->> 'open'       AS o_txt,
               e.val ->> 'high'       AS h_txt,
               e.val ->> 'low'        AS l_txt,
               e.val ->> 'close'      AS c_txt,
               e.val ->> 'volume'     AS v_txt
        FROM jsonb_array_elements(p_rows) AS e(val)
    ),
    typed AS (
        SELECT s.raw,
               CASE WHEN pg_input_is_valid(s.d_txt, 'date')    THEN s.d_txt::date    END AS trade_date,
               CASE WHEN pg_input_is_valid(s.o_txt, 'numeric') THEN s.o_txt::numeric END AS open,
               CASE WHEN pg_input_is_valid(s.h_txt, 'numeric') THEN s.h_txt::numeric END AS high,
               CASE WHEN pg_input_is_valid(s.l_txt, 'numeric') THEN s.l_txt::numeric END AS low,
               CASE WHEN pg_input_is_valid(s.c_txt, 'numeric') THEN s.c_txt::numeric END AS close,
               CASE WHEN pg_input_is_valid(s.v_txt, 'bigint')  THEN s.v_txt::bigint  END AS volume
        FROM src s
    ),
    validated AS (
        SELECT t.*,
               array_remove(ARRAY[
                   CASE WHEN t.trade_date IS NULL THEN 'invalid_trade_date' END,
                   CASE WHEN t.open IS NULL OR t.high IS NULL OR t.low IS NULL OR t.close IS NULL
                        THEN 'missing_or_non_numeric_price' END,
                   CASE WHEN t.volume IS NULL THEN 'missing_or_non_integer_volume' END,
                   -- NaN/Infinity sort above every number, so the upper bound catches them too.
                   CASE WHEN least(t.open, t.high, t.low, t.close) <= 0
                          OR greatest(t.open, t.high, t.low, t.close) >= 100000000
                        THEN 'price_out_of_range' END,
                   CASE WHEN t.high < greatest(t.open, t.close, t.low) THEN 'high_below_open_close_or_low' END,
                   CASE WHEN t.low > least(t.open, t.close, t.high) THEN 'low_above_open_close_or_high' END,
                   CASE WHEN t.volume < 0 THEN 'negative_volume' END,
                   CASE WHEN t.trade_date > market.today_ist() THEN 'future_trade_date' END,
                   CASE WHEN t.trade_date IS NOT NULL AND NOT market.is_trading_day(t.trade_date, v_exchange)
                        THEN 'non_trading_day' END,
                   CASE WHEN t.trade_date IS NOT NULL AND count(*) OVER (PARTITION BY t.trade_date) > 1
                        THEN 'conflicting_duplicate_in_batch' END
               ]::text[], NULL) AS reasons
        FROM typed t
    ),
    q AS (
        INSERT INTO market.ohlcv_quarantine (run_id, instrument_id, trade_date, raw_row, reasons)
        SELECT p_run_id, p_instrument_id, v.trade_date, v.raw, v.reasons
        FROM validated v
        WHERE cardinality(v.reasons) > 0
        RETURNING 1
    ),
    up AS (
        INSERT INTO market.ohlcv_daily AS o
               (instrument_id, trade_date, open, high, low, close, volume, first_run_id, last_run_id)
        SELECT p_instrument_id, v.trade_date, v.open, v.high, v.low, v.close, v.volume, p_run_id, p_run_id
        FROM validated v
        WHERE cardinality(v.reasons) = 0
        ON CONFLICT (instrument_id, trade_date) DO UPDATE
            SET open        = EXCLUDED.open,
                high        = EXCLUDED.high,
                low         = EXCLUDED.low,
                close       = EXCLUDED.close,
                volume      = EXCLUDED.volume,
                last_run_id = EXCLUDED.last_run_id,
                updated_at  = now()
            WHERE (o.open, o.high, o.low, o.close, o.volume)
                  IS DISTINCT FROM (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close, EXCLUDED.volume)
        RETURNING (xmax = 0) AS was_insert
    )
    SELECT (SELECT count(*) FROM up WHERE up.was_insert)::integer,
           (SELECT count(*) FROM up WHERE NOT up.was_insert)::integer,
           ((SELECT count(*) FROM validated v WHERE cardinality(v.reasons) = 0)
             - (SELECT count(*) FROM up))::integer,
           (SELECT count(*) FROM q)::integer;
END;
$$;

-- -----------------------------------------------------------------------------
-- Called after a date range has been fetched from the source:
--   * If the instrument has nothing stored before the first candle we got, the
--     source has no history before that date -> remember it as data_start_date
--     (pre-listing days are then never reported as gaps).
--   * Expected trading days in the range that still have no candle (and were not
--     quarantined) are recorded in market.no_data_dates. Days newer than
--     p_min_age_days are left alone so a late-publishing source gets retried.
-- Returns the number of days marked as "no data".
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION market.record_fetch_result(
    p_run_id        bigint,
    p_instrument_id integer,
    p_from          date,
    p_to            date,
    p_min_age_days  integer DEFAULT 5
)
RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    v_exchange   text;
    v_first_date date;
    v_marked     integer;
BEGIN
    SELECT i.exchange INTO v_exchange FROM market.instruments i WHERE i.instrument_id = p_instrument_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown instrument_id %', p_instrument_id;
    END IF;

    SELECT min(x.d) INTO v_first_date
    FROM (SELECT min(o.trade_date) AS d FROM market.ohlcv_daily o WHERE o.instrument_id = p_instrument_id
          UNION ALL
          SELECT min(q.trade_date) FROM market.ohlcv_quarantine q
          WHERE q.instrument_id = p_instrument_id AND q.resolved_at IS NULL) AS x;

    IF v_first_date IS NOT NULL AND v_first_date > p_from THEN
        UPDATE market.instruments
           SET data_start_date = v_first_date, updated_at = now()
         WHERE instrument_id = p_instrument_id
           AND data_start_date IS DISTINCT FROM v_first_date;
    END IF;

    INSERT INTO market.no_data_dates (instrument_id, trade_date, run_id)
    SELECT p_instrument_id, d, p_run_id
    FROM market.expected_trading_days(greatest(p_from, coalesce(v_first_date, p_from)),
                                      least(p_to, market.today_ist() - p_min_age_days),
                                      v_exchange) AS d
    WHERE NOT EXISTS (SELECT 1 FROM market.ohlcv_daily o
                      WHERE o.instrument_id = p_instrument_id AND o.trade_date = d)
      AND NOT EXISTS (SELECT 1 FROM market.ohlcv_quarantine q
                      WHERE q.instrument_id = p_instrument_id AND q.trade_date = d AND q.resolved_at IS NULL)
    ON CONFLICT (instrument_id, trade_date) DO NOTHING;

    GET DIAGNOSTICS v_marked = ROW_COUNT;
    RETURN v_marked;
END;
$$;

-- -----------------------------------------------------------------------------
-- Re-run validation for unresolved quarantined rows, e.g. after fixing the
-- trading calendar. Rows that now pass are upserted; rows that still fail are
-- quarantined again under p_run_id. Returns totals.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION market.reprocess_quarantine(p_run_id bigint, p_instrument_id integer DEFAULT NULL)
RETURNS TABLE (inserted integer, updated integer, unchanged integer, quarantined integer)
LANGUAGE plpgsql AS $$
DECLARE
    v_instrument_id integer;
    v_rows  jsonb;
    v_res   record;
    v_ins   integer := 0;
    v_upd   integer := 0;
    v_unch  integer := 0;
    v_quar  integer := 0;
BEGIN
    FOR v_instrument_id IN
        SELECT DISTINCT q.instrument_id
        FROM market.ohlcv_quarantine q
        WHERE q.resolved_at IS NULL
          AND (p_instrument_id IS NULL OR q.instrument_id = p_instrument_id)
    LOOP
        WITH picked AS (
            UPDATE market.ohlcv_quarantine q
               SET resolved_at = now()
             WHERE q.resolved_at IS NULL
               AND q.instrument_id = v_instrument_id
            RETURNING q.quarantine_id, q.trade_date, q.raw_row
        ),
        -- Keep only the latest quarantined copy of each day so older copies don't
        -- collide as duplicates; rows with an unparseable date are all kept.
        latest AS (
            SELECT DISTINCT ON (p.trade_date, CASE WHEN p.trade_date IS NULL THEN p.quarantine_id END)
                   p.raw_row
            FROM picked p
            ORDER BY p.trade_date, CASE WHEN p.trade_date IS NULL THEN p.quarantine_id END, p.quarantine_id DESC
        )
        SELECT coalesce(jsonb_agg(l.raw_row), '[]'::jsonb) INTO v_rows FROM latest l;

        SELECT * INTO v_res FROM market.upsert_ohlcv_batch(p_run_id, v_instrument_id, v_rows);
        v_ins  := v_ins  + v_res.inserted;
        v_upd  := v_upd  + v_res.updated;
        v_unch := v_unch + v_res.unchanged;
        v_quar := v_quar + v_res.quarantined;
    END LOOP;

    RETURN QUERY SELECT v_ins, v_upd, v_unch, v_quar;
END;
$$;
