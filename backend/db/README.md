# NSE Market Data Ingestion Pipeline (`backend/db`)

This is an async service that fetches **daily OHLCV** candles for NSE equities
from Angel One SmartAPI and stores them in the PostgreSQL database **`nse_data`**.

- **Fetching:** `httpx` + `asyncio`. Requests pass through an
  `asyncio.BoundedSemaphore` (max in-flight) and a per-second throttle
  (Angel One allows 3 `getCandleData` calls/s). Transient failures are retried
  with exponential backoff and jitter. This covers timeouts, HTTP 429/5xx,
  "exceeding access rate" and Angel One `AB1004`/`AB2001`. An expired token
  triggers a single re-login.
- **Storing:** every write goes through the PostgreSQL stored function
  `market.upsert_ohlcv_batch()`. It validates each row, quarantines bad rows
  and idempotently upserts the good ones, so you can re-run any batch safely.
- **Gap detection:** `market.detect_gaps()` compares stored candles with a
  trading calendar (weekdays − NSE holidays + special sessions). It returns
  the missing date ranges, which the pipeline then backfills automatically.
- **Pandas** parses and normalises the raw candles (timestamps → IST trade date,
  numeric coercion, exact-duplicate removal) before they go to the database.

For a full explanation of every table, stored function and how a run works,
see [DATABASE_GUIDE.md](DATABASE_GUIDE.md).

```
db/
├── README.md                  ← this file (all steps)
├── DATABASE_GUIDE.md          ← tables, data sources, functions, run flow explained
├── watchlist.txt              ← default symbols to track (NIFTY 50)
├── sql/
│   ├── 00_create_database.sql ← CREATE DATABASE nse_data
│   ├── 01_schema.sql          ← schema "market" + all tables
│   ├── 02_trading_calendar.sql← NSE holidays & special sessions (seed data)
│   └── 03_functions.sql       ← stored functions (gap detection, validated upsert, ...)
└── pipeline/                  ← Python service (python -m db.pipeline ...)
    ├── angel_http.py          ← async httpx Angel One client (semaphore, throttle, retries)
    ├── transform.py           ← pandas normalisation
    ├── repository.py          ← asyncpg access + DB setup
    ├── ingest.py              ← orchestration (gaps → fetch → validate → upsert)
    └── __main__.py            ← CLI
```

---

## Step 1 — Configure `backend/.env`

Fill in the Angel One credentials (also used by the dashboard API) and the database settings:

```ini
ANGEL_API_KEY=...
ANGEL_CLIENT_CODE=...
ANGEL_PASSWORD=...          # your 4-digit Angel One MPIN (not the trading password)
ANGEL_TOTP_SECRET=...       # base32 secret shown when enabling TOTP

DB_HOST=172.16.1.30
DB_PORT=5432
DB_USER_NAME=devteam
DB_PASSWORD=...
DB_NAME=nse_data
```

Optional tuning (defaults shown):

```ini
INGEST_MAX_CONCURRENCY=3          # bounded-semaphore size (in-flight requests)
INGEST_REQUESTS_PER_SECOND=3      # throttle
INGEST_MAX_RETRIES=5
INGEST_BACKFILL_START=2020-01-01  # default --from for backfills
```

> ⚠️ Angel One locks the account after several wrong MPIN attempts. The pipeline
> logs in **once** at the start of a run and stops immediately if the login is
> rejected. It never retries a rejected login.

## Step 2 — Install dependencies

Run these from `backend/`:

```bash
pip install -r requirements.txt     # adds asyncpg + pandas to the existing deps
```

All commands below are also run from `backend/`.

## Step 3 — Create the database, tables and stored functions

```bash
python -m db.pipeline init-db
```

This creates `nse_data` if it does not exist, then applies
`sql/01_schema.sql`, `sql/02_trading_calendar.sql` and `sql/03_functions.sql`.
Every file is idempotent, so re-run it whenever the SQL changes (for example,
after adding next year's holidays).

You can do the same with `psql` instead:

```bash
psql -h 172.16.1.30 -U devteam -d postgres -f db/sql/00_create_database.sql
psql -h 172.16.1.30 -U devteam -d nse_data -f db/sql/01_schema.sql
psql -h 172.16.1.30 -U devteam -d nse_data -f db/sql/02_trading_calendar.sql
psql -h 172.16.1.30 -U devteam -d nse_data -f db/sql/03_functions.sql
```

Requires PostgreSQL 16+ (the validation uses `pg_input_is_valid`).

## Step 4 — Load the NSE instrument list

```bash
python -m db.pipeline sync-instruments
```

This downloads Angel One's scrip master and upserts every NSE equity (`*-EQ`,
about 2,700) into `market.instruments`. Angel One identifies instruments by
`symbol_token`, so this lookup is needed before fetching candles. Re-run it
occasionally to pick up new listings.

## Step 5 — Choose which symbols to track

```bash
python -m db.pipeline track --file db/watchlist.txt     # NIFTY 50
python -m db.pipeline track HDFCBANK SBIN               # or individual tickers
python -m db.pipeline untrack SBIN
```

## Step 6 — Fetch and store OHLCV data

```bash
python -m db.pipeline run                               # all tracked symbols, backfill from INGEST_BACKFILL_START
python -m db.pipeline run RELIANCE TCS --from 2024-01-01
python -m db.pipeline run --recheck-days 7              # also re-fetch the last 7 days to pick up corrections
```

What a run does, for each instrument (several in parallel):

1. `market.detect_gaps(instrument, from, to)` returns ranges of expected trading
   days with no stored candle. On the first run this is the whole history; after
   that it is usually just the days since the last run.
2. Each range is fetched with `getCandleData` (`ONE_DAY`), split into windows of
   at most 2000 days, going through the semaphore, throttle and retries.
3. Pandas normalises the candles. `market.upsert_ohlcv_batch()` validates every
   row: invalid rows go to `market.ohlcv_quarantine` with reasons, and valid
   rows are inserted or updated (unchanged rows are left alone).
4. `market.record_fetch_result()` records the instrument's first available date
   (listing date), so pre-listing days are never reported as gaps. Expected days
   the source had no candle for are added to `market.no_data_dates`, so they are
   not requested again.

`--to` defaults to the last *completed* session (today after 16:00 IST,
otherwise yesterday), so a partial intraday candle is never stored. An advisory
lock stops two runs from overlapping. Every run is logged in
`market.ingestion_runs`, with one row per symbol in `market.ingestion_run_items`.

Exit codes: `0` success, `1` partial (some symbols failed), `2` bad arguments,
`3` another run is active, `4` all symbols failed, `5` Angel One login rejected.

## Step 7 — Schedule the daily update

Because runs only fetch missing days, schedule the same command every weekday evening:

- **Windows Task Scheduler:** Program `python`, arguments `-m db.pipeline run --recheck-days 5`,
  start in `D:\NSE-Market-Data-Ingestion-Pipeline\backend`, trigger Mon–Fri 18:30.
- **cron (Linux):** `30 18 * * 1-5 cd /path/to/backend && python -m db.pipeline run --recheck-days 5`

## Step 8 — Inspect and maintain

```bash
python -m db.pipeline status                 # counts + last 5 runs
python -m db.pipeline gaps RELIANCE          # show missing ranges without fetching
python -m db.pipeline reprocess-quarantine   # re-validate quarantined rows (e.g. after a calendar fix)
```

Useful queries:

```sql
-- Daily candles for a symbol
SELECT o.trade_date, o.open, o.high, o.low, o.close, o.volume
FROM market.ohlcv_daily o JOIN market.instruments i USING (instrument_id)
WHERE i.symbol = 'RELIANCE-EQ' ORDER BY o.trade_date DESC LIMIT 20;

-- What was quarantined, and why
SELECT i.symbol, q.trade_date, q.reasons, q.raw_row
FROM market.ohlcv_quarantine q JOIN market.instruments i USING (instrument_id)
WHERE q.resolved_at IS NULL ORDER BY q.quarantined_at DESC;

-- Last runs
SELECT * FROM market.ingestion_runs ORDER BY run_id DESC LIMIT 10;
```

---

## Database reference (schema `market`)

| Table | Purpose |
| --- | --- |
| `instruments` | NSE equities (`symbol_token`, `symbol`, `is_tracked`, `data_start_date`) |
| `ohlcv_daily` | Clean daily candles, PK `(instrument_id, trade_date)`, with CHECK constraints |
| `ohlcv_quarantine` | Rows that failed validation: raw JSON + `reasons[]` |
| `no_data_dates` | Expected trading days the source confirmed it has no candle for |
| `trading_holidays` / `special_sessions` | NSE trading calendar |
| `ingestion_runs` / `ingestion_run_items` | Per-run and per-symbol audit trail |

| Function | Purpose |
| --- | --- |
| `is_trading_day(date)` / `expected_trading_days(from, to)` | Calendar |
| `detect_gaps(instrument_id, from, to)` | Missing trading-day ranges (gaps-and-islands) |
| `upsert_ohlcv_batch(run_id, instrument_id, rows jsonb)` | Row-level validation + quarantine + idempotent upsert; returns inserted/updated/unchanged/quarantined |
| `record_fetch_result(run_id, instrument_id, from, to)` | Sets listing date, marks "no data" days |
| `reprocess_quarantine(run_id)` | Re-validates unresolved quarantined rows |

**Validation rules** (each failing row is quarantined with every matching reason):
`invalid_trade_date`, `missing_or_non_numeric_price`, `missing_or_non_integer_volume`,
`price_out_of_range` (≤ 0, NaN or Infinity), `high_below_open_close_or_low`,
`low_above_open_close_or_high`, `negative_volume`, `future_trade_date`,
`non_trading_day`, `conflicting_duplicate_in_batch`.

**Trading calendar:** `02_trading_calendar.sql` seeds the NSE holidays for 2024–2026.
Add each new year from the NSE circular and re-run `init-db`. Years not in the
calendar still work: unknown holidays are requested once, come back empty and
are recorded in `no_data_dates`.
