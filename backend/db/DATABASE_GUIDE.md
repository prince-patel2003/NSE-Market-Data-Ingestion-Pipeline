# NSE OHLCV Pipeline: Database & Functionality Guide

This guide explains how the ingestion pipeline works. It covers:

- every table in the `nse_data` database and what it is for,
- where each table's data comes from (automatic or manual),
- what the stored functions do,
- how a run works from start to finish.

For the commands to set things up, see [README.md](README.md).

---

## 1. The big picture

The pipeline downloads **daily OHLCV** candles (Open, High, Low, Close, Volume)
for NSE stocks from **Angel One SmartAPI** and stores them in **PostgreSQL**.
It is built to be re-run safely at any time: each run works out what is
missing, fetches only that, checks every row, and stores good rows. Bad rows
are set aside.

```
                    ┌──────────────────────────────┐
                    │      Angel One SmartAPI      │
                    │  • Scrip master (stock list) │
                    │  • Login (MPIN + TOTP)       │
                    │  • getCandleData (ONE_DAY)   │
                    └──────────────┬───────────────┘
                                   │ httpx (async)
                                   │ bounded semaphore + 3 req/s throttle + retries
                    ┌──────────────▼───────────────┐
                    │  Python pipeline (db/pipeline)│
                    │  1. find gaps   (SQL function)│
                    │  2. fetch candles             │
                    │  3. normalise   (pandas)      │
                    │  4. store       (SQL function)│
                    └──────────────┬───────────────┘
                                   │ asyncpg
                    ┌──────────────▼───────────────┐
                    │ PostgreSQL: nse_data / market │
                    │  ohlcv_daily      ← good rows │
                    │  ohlcv_quarantine ← bad rows  │
                    │  no_data_dates, runs, calendar│
                    └──────────────────────────────┘
```

All tables live in one schema: **`market`**.

---

## 2. Where does the data come from? (automatic vs manual)

| Table | Filled by | Manual work needed? |
| --- | --- | --- |
| `instruments` | **Automatic.** `sync-instruments` downloads Angel One's scrip master. | Only choosing **which** stocks to track (`track` command) |
| `trading_holidays` | **Seed file** `sql/02_trading_calendar.sql` | **Yes, once a year:** add the next year's NSE holidays |
| `special_sessions` | **Seed file** `sql/02_trading_calendar.sql` | **Yes, when NSE announces one** (Budget-day weekend session, Muhurat trading) |
| `ohlcv_daily` | **Automatic.** `run` → `market.upsert_ohlcv_batch()` | No. Never insert here by hand. |
| `ohlcv_quarantine` | **Automatic.** Rows that fail validation | Review now and then; optionally re-check with `reprocess-quarantine` |
| `no_data_dates` | **Automatic.** `market.record_fetch_result()` | No (delete rows only if you want to force a re-fetch) |
| `ingestion_runs` | **Automatic.** One row per `run` | No |
| `ingestion_run_items` | **Automatic.** One row per stock per run | No |

### What you do by hand: the complete list

1. **Fill in `backend/.env`.** Angel One credentials (API key, client code, MPIN, TOTP secret) and DB connection details.
2. **Create the database once:** `python -m db.pipeline init-db`.
3. **Load the stock list once** (and occasionally to pick up new listings): `python -m db.pipeline sync-instruments`.
4. **Choose stocks to track:** `python -m db.pipeline track --file db/watchlist.txt` or `track RELIANCE TCS`.
5. **Run it or schedule it:** `python -m db.pipeline run` (for example every weekday at 18:30).
6. **Once a year:** add the new NSE holiday list to `sql/02_trading_calendar.sql` and re-run `init-db`.
7. **Optional:** review `ohlcv_quarantine`.

Everything else, including all price data, is gathered and inserted automatically.

---

## 3. Tables in detail

### 3.1 `market.instruments`: the list of stocks

**Purpose:** the master list of NSE equities. Angel One's candle API does not
take a ticker like `RELIANCE`. It needs a numeric **symbol token** (for
example `2885`), so this table maps tickers to tokens. It also records which
stocks you want the pipeline to ingest.

| Column | Meaning | Source |
| --- | --- | --- |
| `instrument_id` | Internal ID, used as the foreign key everywhere else | auto (serial) |
| `symbol_token` | Angel One's token (unique) | scrip master |
| `symbol` | Trading symbol, e.g. `RELIANCE-EQ` | scrip master |
| `name` | Short name, e.g. `RELIANCE` | scrip master |
| `exchange` | Always `NSE` here | scrip master |
| `is_tracked` | `true` = included in `run` when no symbols are given | **you**, via `track` / `untrack` |
| `data_start_date` | Earliest date Angel One has candles for (≈ listing date) | auto, `record_fetch_result()` |
| `created_at`, `updated_at` | Timestamps | auto |

**How it is filled:** `sync-instruments` downloads
`OpenAPIScripMaster.json` (all exchanges, ~100k rows) and keeps only rows with
`exch_seg = 'NSE'` and a symbol ending in `-EQ` (about 2,700 equities). They
are bulk-loaded with `COPY` into a temp table and upserted on `symbol_token`.
Existing rows update only if the symbol or name changed, and your
`is_tracked` flags are never reset. If the table is empty, `run` and `track`
also trigger this sync automatically.

### 3.2 `market.ohlcv_daily`: the clean price data (main table)

**Purpose:** one row per stock per trading day. **This is the table you query
for analysis and charts.**

| Column | Meaning |
| --- | --- |
| `instrument_id`, `trade_date` | **Primary key.** At most one candle per stock per day. |
| `open`, `high`, `low`, `close` | Prices, `numeric(14,4)` (exact decimals, no float rounding) |
| `volume` | Shares traded, `bigint` |
| `source` | `angelone` |
| `first_run_id` | The run that first inserted this row |
| `last_run_id` | The last run that changed its values |
| `created_at`, `updated_at` | Timestamps |

**Safety net:** CHECK constraints reject impossible candles even if someone
bypasses the pipeline:
prices > 0, `high ≥ open/close/low`, `low ≤ open/close`, `volume ≥ 0`.

**How it is filled:** only through `market.upsert_ohlcv_batch()`, never by hand.

### 3.3 `market.ohlcv_quarantine`: rejected rows

**Purpose:** rows that failed validation are kept here with the **raw data
exactly as received** and **every reason they failed**. Bad data never reaches
`ohlcv_daily`, but nothing is silently lost.

| Column | Meaning |
| --- | --- |
| `quarantine_id` | ID |
| `run_id` | Run that produced it |
| `instrument_id` | Stock |
| `trade_date` | Parsed date (`NULL` if the date itself was unreadable) |
| `raw_row` | JSON: the normalised values plus `source_row` (the original Angel One candle) |
| `reasons` | Text array, e.g. `{high_below_open_close_or_low}` |
| `quarantined_at` | When |
| `resolved_at` | Set when `reprocess-quarantine` picked it up again |

Example row:

```
trade_date = 2025-01-06
reasons    = {high_below_open_close_or_low, low_above_open_close_or_high}
raw_row    = {"trade_date":"2025-01-06","open":100,"high":90,"low":95,"close":105,"volume":1000,
              "source_row":{"timestamp":"2025-01-06T00:00:00+05:30","open":100,"high":90,...}}
```

### 3.4 `market.no_data_dates`: days the source has nothing for

**Purpose:** sometimes a day the calendar says is a trading day has no candle
from Angel One. This happens when a stock is suspended, or on a holiday that
is missing from the calendar. Without this table, the pipeline would request
those days on **every run, forever**. Once a day is recorded here, gap
detection skips it.

| Column | Meaning |
| --- | --- |
| `instrument_id`, `trade_date` | Primary key |
| `run_id`, `checked_at` | Which run confirmed it, and when |

**Protection against late data:** days from the **last 5 days** are never
marked, in case Angel One publishes them late. They are simply retried next
run.

**Force a re-fetch:** `DELETE FROM market.no_data_dates WHERE ...;` and run again.

### 3.5 `market.trading_holidays` and `market.special_sessions`: the trading calendar

**Purpose:** these tell the pipeline which days **should** have a candle.

- `trading_holidays (exchange, holiday_date, description)`: weekdays when NSE is closed.
- `special_sessions (exchange, session_date, description)`: days NSE trades even
  though they are a weekend or holiday (e.g. Union Budget Saturday 2025-02-01,
  Budget Sunday 2026-02-01, Muhurat trading).

A day is a **trading day** if it is a special session, **or** it is Monday to
Friday and not a holiday.

**How it is filled:** **manually**, through `sql/02_trading_calendar.sql`
(2024–2026 are included). Each year, copy the list from the NSE holiday
circular into that file and re-run `init-db`. The file uses
`ON CONFLICT DO UPDATE`, so re-running it is safe.

**If the calendar is incomplete:** nothing breaks.
- A missing holiday: that day is requested once, comes back empty, and is
  recorded in `no_data_dates`.
- A missing special session: its candle is quarantined as `non_trading_day`.
  Add the session to the calendar, then run `reprocess-quarantine`, and the
  candle moves into `ohlcv_daily`.

### 3.6 `market.ingestion_runs` and `market.ingestion_run_items`: audit trail

**Purpose:** a history of every run, so you can see what happened, when, and
what failed.

`ingestion_runs` has one row per `run` command:

| Column | Meaning |
| --- | --- |
| `run_id` | ID (also stamped on candles, quarantine and no-data rows) |
| `started_at`, `finished_at` | Timing |
| `status` | `running` → `success` / `partial` (some stocks failed) / `failed` |
| `from_date`, `to_date` | Requested date window |
| `instruments_total`, `instruments_failed` | Counts |
| `rows_fetched / inserted / updated / unchanged / quarantined` | Totals summed from the items |
| `params` | JSON of the command options (symbols, recheck_days) |
| `error` | Failed symbols or the exception |

`ingestion_run_items` has one row per stock per run, with the same counters plus
`gap_ranges`, `missing_days`, `no_data_days` and that stock's own `status`
(`success`, `up_to_date`, `failed`) and `error`.

---

## 4. Stored functions (the database's logic)

All validation and storage rules live **in the database**, in
`sql/03_functions.sql`, so they are applied the same way no matter who calls them.

### 4.1 `market.is_trading_day(date)` and `market.expected_trading_days(from, to)`

These are the calendar rules from §3.5. `expected_trading_days` returns every
trading day in a range.

### 4.2 `market.detect_gaps(instrument_id, from, to)`: calendar-based gap detection

It returns ranges of trading days that have **no** candle in `ohlcv_daily` and
are **not** in `no_data_dates`. The start is moved up to the stock's
`data_start_date`, so pre-listing years never show up as gaps.

It uses the "gaps and islands" technique: number all trading days, number the
missing ones, and subtract. Consecutive missing days share the same difference,
so they group into one range. Weekends and holidays do not split a range,
because they are not trading days.

```
Trading days:   Thu2  Fri3  Mon6  Tue7  Wed8  Thu9  Fri10
Stored?          ✔     ✔     ✘     ✘     ✔     ✘     ✘
Result:  (Mon6 .. Tue7, 2 days), (Thu9 .. Fri10, 2 days)   → 2 API requests
```

- **First run:** the whole history is one gap, a full **backfill**.
- **Daily runs after that:** usually only the last day or so.

### 4.3 `market.upsert_ohlcv_batch(run_id, instrument_id, rows jsonb)`: validate, quarantine, store

This is the heart of the pipeline. It takes a whole batch (a JSON array of
candles) and does the following in **one SQL statement**:

1. **Locks** the stock (`pg_advisory_xact_lock`) so two writers cannot clash.
2. **Type-checks** every field with `pg_input_is_valid`. Unreadable values become `NULL` instead of crashing the batch.
3. **Validates** each row and collects **all** failure reasons:

   | Reason | Rule |
   | --- | --- |
   | `invalid_trade_date` | Date missing or unparseable |
   | `missing_or_non_numeric_price` | Any of O/H/L/C missing or not a number |
   | `missing_or_non_integer_volume` | Volume missing or fractional |
   | `price_out_of_range` | A price ≤ 0, ≥ 10 crore, NaN or Infinity |
   | `high_below_open_close_or_low` | High is not the highest value |
   | `low_above_open_close_or_high` | Low is not the lowest value |
   | `negative_volume` | Volume < 0 |
   | `future_trade_date` | Date after today (IST) |
   | `non_trading_day` | Weekend or holiday per the calendar |
   | `conflicting_duplicate_in_batch` | Same date appears twice with **different** values (both copies quarantined) |

4. Inserts rows with any reason into **`ohlcv_quarantine`**.
5. **Upserts** valid rows into **`ohlcv_daily`** with `ON CONFLICT (instrument_id, trade_date) DO UPDATE ... WHERE values IS DISTINCT FROM`:
   - new day → **inserted**
   - existing day with different values (Angel One corrected it) → **updated**
   - existing day with identical values → **untouched** (counted as unchanged)
6. Returns `inserted, updated, unchanged, quarantined`.

**Why it is safe to repeat (idempotent):** sending the same batch twice
gives `inserted=0, updated=0, unchanged=N` the second time. Nothing is
duplicated or rewritten, so any run or batch can be retried after a crash.

### 4.4 `market.record_fetch_result(run_id, instrument_id, from, to)`

This is called after each range has been fetched.

- **Listing date:** if the earliest stored candle is later than the date we
  asked for, Angel One has nothing before it. It is saved as
  `instruments.data_start_date`.
- **No-data days:** trading days in the range that still have no candle (and
  were not quarantined) go into `no_data_dates`. The last 5 days are skipped.

### 4.5 `market.reprocess_quarantine(run_id)`

This takes all unresolved quarantined rows, marks them resolved, and runs them
through `upsert_ohlcv_batch` again. Rows that now pass (for example after a
calendar fix) move into `ohlcv_daily`. Rows that still fail are quarantined
again under the new run. If a day was quarantined more than once, only the
latest copy is reprocessed.

### 4.6 `market.today_ist()`

Returns today's date in India. The DB server may run in UTC.

---

## 5. How a run works, step by step

The command is `python -m db.pipeline run`.

```
 1. Decide the window
      from = --from  or INGEST_BACKFILL_START (default 2020-01-01)
      to   = --to    or the last completed session
             (today if it is after 16:00 IST, otherwise yesterday, so a
              half-finished intraday candle is never stored)

 2. Take the pipeline lock      pg_try_advisory_lock → a second run exits (code 3)

 3. Pick the stocks             given symbols, or every is_tracked = true

 4. Log in to Angel One ONCE    a wrong MPIN stops the run here (code 5) and is
                                never retried, because repeated wrong MPINs lock
                                the account

 5. Insert ingestion_runs row (status = running)

 6. For each stock (6 at a time):
      a. ingestion_run_items row
      b. gaps = market.detect_gaps(...)
      c. (--recheck-days N) also re-fetch the last N days to pick up corrections
      d. for each gap range:
           fetch candles   → httpx, split into ≤2000-day requests
           pandas          → IST trade_date, numeric types, drop exact duplicates
           store           → market.upsert_ohlcv_batch(...)
           bookkeeping     → market.record_fetch_result(...)
      e. update the run item: success / up_to_date / failed (+ error)
         A failure of one stock does not stop the others.

 7. Finish the run: totals summed, status = success / partial / failed
```

### 5.1 Rate control: the bounded semaphore and the throttle

Angel One allows about **3 candle requests per second**. Two controls work together:

- **`asyncio.BoundedSemaphore(3)`:** at most 3 requests are in flight at the
  same time. More stocks are processed in parallel, but they wait for a free
  slot.
- **Request throttle:** request start times are spaced at least
  `1 / INGEST_REQUESTS_PER_SECOND` seconds apart. This prevents bursts.

Both are configurable in `.env` (`INGEST_MAX_CONCURRENCY`, `INGEST_REQUESTS_PER_SECOND`).

### 5.2 Retry logic

| Situation | What happens |
| --- | --- |
| Timeout / network error | Retry |
| HTTP 429, 500, 502, 503, 504 | Retry (honours `Retry-After`) |
| "Access denied because of exceeding access rate" | Retry |
| Angel One `AB1004`, `AB2001` (temporary server errors) | Retry |
| Token expired (`AG8001/2/3`, `AB1010/11`, HTTP 401/403) | Log in again once, then retry |
| Login rejected (wrong MPIN/TOTP) | **Stop the whole run, no retry** |
| Any other API error | That stock fails; the others continue |

Waits grow exponentially: 1s, 2s, 4s, 8s… (capped at 30s), plus random jitter
so parallel requests do not retry at the same moment. The maximum is
`INGEST_MAX_RETRIES` attempts (default 5).

### 5.3 What pandas does

`transform.py` turns Angel One's raw rows
(`["2025-01-02T00:00:00+05:30", 1200.5, 1210, 1195, 1205.2, 345678]`) into clean records:

- timestamp → trade date in **IST**
- prices and volume → numbers. Anything unreadable becomes `NULL`, so the
  database quarantines it with a reason instead of the run crashing.
- rows with too few or too many fields are padded or trimmed
- **exact** duplicates are dropped (harmless repeats). **Conflicting**
  duplicates are kept so the database can quarantine them.
- the original raw row is attached as `source_row` for the quarantine record

Pandas only cleans the data. **All accept/reject decisions are made in the
database function.**

---

## 6. Example timeline

| Day | Action | Result |
| --- | --- | --- |
| Day 1 | `run` (first time, from 2020-01-01) | 1 gap per stock (whole history). ~1,650 candles per stock inserted. Pre-listing dates set. Unknown-holiday days recorded in `no_data_dates`. |
| Day 1 (again) | `run` | All stocks `up_to_date`. 0 API calls for candles. |
| Day 2, 18:30 | scheduled `run --recheck-days 5` | Gap = yesterday only, so 1 new candle per stock. Last 5 days re-fetched; any Angel One corrections are **updated**. |
| Any day | Angel One sends a broken candle | Goes to `ohlcv_quarantine`. The day stays a gap and is retried next run. |
| New year | Add the holidays to `02_trading_calendar.sql`, run `init-db` | Calendar updated. |

---

## 7. Common tasks

```sql
-- Latest 20 candles for a stock
SELECT o.trade_date, o.open, o.high, o.low, o.close, o.volume
FROM market.ohlcv_daily o JOIN market.instruments i USING (instrument_id)
WHERE i.symbol = 'RELIANCE-EQ'
ORDER BY o.trade_date DESC LIMIT 20;

-- Coverage per tracked stock
SELECT i.symbol, count(o.*) AS candles, min(o.trade_date), max(o.trade_date)
FROM market.instruments i LEFT JOIN market.ohlcv_daily o USING (instrument_id)
WHERE i.is_tracked GROUP BY i.symbol ORDER BY i.symbol;

-- Open quarantine items
SELECT i.symbol, q.trade_date, q.reasons, q.raw_row
FROM market.ohlcv_quarantine q JOIN market.instruments i USING (instrument_id)
WHERE q.resolved_at IS NULL ORDER BY q.quarantined_at DESC;

-- Failed stocks in the last run
SELECT i.symbol, r.error
FROM market.ingestion_run_items r JOIN market.instruments i USING (instrument_id)
WHERE r.run_id = (SELECT max(run_id) FROM market.ingestion_runs) AND r.status = 'failed';

-- Track a stock with SQL instead of the CLI
UPDATE market.instruments SET is_tracked = true WHERE symbol = 'ZOMATO-EQ';

-- Re-download a stock's whole history (the next run backfills it)
DELETE FROM market.ohlcv_daily   WHERE instrument_id = 123;
DELETE FROM market.no_data_dates WHERE instrument_id = 123;
UPDATE market.instruments SET data_start_date = NULL WHERE instrument_id = 123;
```

| Command | Purpose |
| --- | --- |
| `python -m db.pipeline init-db` | Create the DB, tables, calendar and functions (safe to repeat) |
| `python -m db.pipeline sync-instruments` | Refresh the stock list |
| `python -m db.pipeline track/untrack ...` | Choose the stocks to ingest |
| `python -m db.pipeline run [symbols] [--from] [--to] [--recheck-days N]` | Fetch and store |
| `python -m db.pipeline gaps [symbols]` | Show missing ranges without fetching |
| `python -m db.pipeline reprocess-quarantine` | Re-validate quarantined rows |
| `python -m db.pipeline status` | Counts and the last 5 runs |

---

## 8. Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| `AB1007 INVALID MPIN` (exit code 5) | `ANGEL_PASSWORD` must be your 4-digit Angel One **MPIN**. Fix it before retrying: several wrong attempts lock the account. |
| Login error about TOTP | `ANGEL_TOTP_SECRET` must be the base32 secret from enabling TOTP, and the PC clock must be correct. |
| "Another ingestion run is in progress" (exit code 3) | A run is still active, or crashed while holding the lock. The lock is released automatically when its DB connection closes. |
| `Unknown NSE symbol 'X' (skipped)` | The ticker is not in `instruments`. Check the spelling or run `sync-instruments`. |
| Many `non_trading_day` quarantines on one date | That date is a special session. Add it to `special_sessions`, then run `init-db` and `reprocess-quarantine`. |
| A stock keeps showing the same gap | Its candle for that day keeps failing validation. Check `ohlcv_quarantine`. |
