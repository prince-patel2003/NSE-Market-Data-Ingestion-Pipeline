-- =============================================================================
-- 01_schema.sql — tables for the NSE OHLCV ingestion pipeline.
-- Idempotent: safe to run any number of times.
-- Run against the nse_data database (see 00_create_database.sql).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS market;

-- -----------------------------------------------------------------------------
-- Instruments (NSE equities from Angel One's scrip master)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.instruments (
    instrument_id   serial      PRIMARY KEY,
    symbol_token    text        NOT NULL UNIQUE,          -- Angel One "symboltoken"
    symbol          text        NOT NULL,                 -- e.g. RELIANCE-EQ
    name            text        NOT NULL,                 -- e.g. RELIANCE
    exchange        text        NOT NULL DEFAULT 'NSE',
    is_tracked      boolean     NOT NULL DEFAULT false,   -- included in scheduled ingestion runs
    data_start_date date,                                 -- earliest date the source has candles for
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_instruments_symbol  ON market.instruments (exchange, upper(symbol));
CREATE INDEX IF NOT EXISTS ix_instruments_tracked ON market.instruments (is_tracked) WHERE is_tracked;

-- -----------------------------------------------------------------------------
-- Trading calendar
-- -----------------------------------------------------------------------------
-- Weekdays that are exchange holidays.
CREATE TABLE IF NOT EXISTS market.trading_holidays (
    exchange     text NOT NULL DEFAULT 'NSE',
    holiday_date date NOT NULL,
    description  text NOT NULL,
    PRIMARY KEY (exchange, holiday_date)
);

-- Days the exchange traded even though they are a weekend or holiday
-- (Budget-day Saturday/Sunday sessions, Muhurat trading, ...).
CREATE TABLE IF NOT EXISTS market.special_sessions (
    exchange     text NOT NULL DEFAULT 'NSE',
    session_date date NOT NULL,
    description  text NOT NULL,
    PRIMARY KEY (exchange, session_date)
);

-- -----------------------------------------------------------------------------
-- Ingestion run bookkeeping
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.ingestion_runs (
    run_id            bigserial   PRIMARY KEY,
    started_at        timestamptz NOT NULL DEFAULT now(),
    finished_at       timestamptz,
    status            text        NOT NULL DEFAULT 'running'
                      CHECK (status IN ('running', 'success', 'partial', 'failed')),
    from_date         date        NOT NULL,
    to_date           date        NOT NULL,
    instruments_total integer     NOT NULL DEFAULT 0,
    instruments_failed integer    NOT NULL DEFAULT 0,
    rows_fetched      integer     NOT NULL DEFAULT 0,
    rows_inserted     integer     NOT NULL DEFAULT 0,
    rows_updated      integer     NOT NULL DEFAULT 0,
    rows_unchanged    integer     NOT NULL DEFAULT 0,
    rows_quarantined  integer     NOT NULL DEFAULT 0,
    params            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    error             text
);

CREATE TABLE IF NOT EXISTS market.ingestion_run_items (
    run_id           bigint      NOT NULL REFERENCES market.ingestion_runs (run_id) ON DELETE CASCADE,
    instrument_id    integer     NOT NULL REFERENCES market.instruments (instrument_id),
    status           text        NOT NULL DEFAULT 'running'
                     CHECK (status IN ('running', 'success', 'up_to_date', 'failed')),
    gap_ranges       integer     NOT NULL DEFAULT 0,
    missing_days     integer     NOT NULL DEFAULT 0,
    rows_fetched     integer     NOT NULL DEFAULT 0,
    rows_inserted    integer     NOT NULL DEFAULT 0,
    rows_updated     integer     NOT NULL DEFAULT 0,
    rows_unchanged   integer     NOT NULL DEFAULT 0,
    rows_quarantined integer     NOT NULL DEFAULT 0,
    no_data_days     integer     NOT NULL DEFAULT 0,
    error            text,
    started_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    PRIMARY KEY (run_id, instrument_id)
);

-- -----------------------------------------------------------------------------
-- Daily OHLCV candles (the clean, validated data)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.ohlcv_daily (
    instrument_id integer        NOT NULL REFERENCES market.instruments (instrument_id),
    trade_date    date           NOT NULL,
    open          numeric(14, 4) NOT NULL,
    high          numeric(14, 4) NOT NULL,
    low           numeric(14, 4) NOT NULL,
    close         numeric(14, 4) NOT NULL,
    volume        bigint         NOT NULL,
    source        text           NOT NULL DEFAULT 'angelone',
    first_run_id  bigint         REFERENCES market.ingestion_runs (run_id),
    last_run_id   bigint         REFERENCES market.ingestion_runs (run_id),
    created_at    timestamptz    NOT NULL DEFAULT now(),
    updated_at    timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, trade_date),
    -- Last line of defence; market.upsert_ohlcv_batch() quarantines such rows before they get here.
    CONSTRAINT ck_ohlcv_positive_prices CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0),
    CONSTRAINT ck_ohlcv_high_low        CHECK (high >= low AND high >= open AND high >= close
                                               AND low <= open AND low <= close),
    CONSTRAINT ck_ohlcv_volume          CHECK (volume >= 0)
);

CREATE INDEX IF NOT EXISTS ix_ohlcv_daily_trade_date ON market.ohlcv_daily (trade_date);

-- -----------------------------------------------------------------------------
-- Quarantine: rows that failed row-level validation, kept with the raw payload
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.ohlcv_quarantine (
    quarantine_id  bigserial   PRIMARY KEY,
    run_id         bigint      REFERENCES market.ingestion_runs (run_id),
    instrument_id  integer     NOT NULL REFERENCES market.instruments (instrument_id),
    trade_date     date,                      -- NULL when the date itself was unparseable
    raw_row        jsonb       NOT NULL,
    reasons        text[]      NOT NULL,
    quarantined_at timestamptz NOT NULL DEFAULT now(),
    resolved_at    timestamptz                -- set when re-processed (see market.reprocess_quarantine)
);

CREATE INDEX IF NOT EXISTS ix_quarantine_unresolved
    ON market.ohlcv_quarantine (instrument_id, trade_date) WHERE resolved_at IS NULL;

-- -----------------------------------------------------------------------------
-- Expected trading days for which the source confirmed it has no candle
-- (suspensions, holidays missing from the calendar, ...). Gap detection skips them,
-- so the pipeline does not re-request the same empty days on every run.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.no_data_dates (
    instrument_id integer     NOT NULL REFERENCES market.instruments (instrument_id),
    trade_date    date        NOT NULL,
    run_id        bigint      REFERENCES market.ingestion_runs (run_id),
    checked_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, trade_date)
);
