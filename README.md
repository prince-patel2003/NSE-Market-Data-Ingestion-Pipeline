# NSE Market Dashboard

A FastAPI + React app that shows live quotes and historical OHLCV candles for NSE
stocks, using the [Angel One SmartAPI](https://smartapi.angelbroking.com/).

- `backend/` — FastAPI service that logs in to Angel One SmartAPI and exposes
  `/api/symbols`, `/api/quote/{symbol}` and `/api/historical/{symbol}`.
- `frontend/` — React (Vite + Tailwind) dashboard: search a symbol, see its live
  LTP/OHLC, and a candlestick chart with selectable interval/lookback.
- `backend/db/` — async ingestion pipeline (httpx/asyncio, PostgreSQL, Pandas)
  that backfills and maintains daily OHLCV for tracked NSE stocks in the
  `nse_data` PostgreSQL database. It uses a bounded-semaphore rate limit,
  retries, calendar-based gap detection, and a stored function that validates
  rows and quarantines bad data. See [backend/db/README.md](backend/db/README.md)
  for setup steps.

## Prerequisites

- An Angel One SmartAPI account with an API key (create an app at
  https://smartapi.angelbroking.com/), your client code, your login PIN/MPIN,
  and the TOTP secret shown when you enable 2FA for the API.
- Python 3.11+ and Node.js 18+.

## Backend setup

```bash
cd backend
python -m venv .venv
.venv/Scripts/activate        # on macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env           # then fill in your real Angel One credentials
uvicorn app.main:app --reload --port 8000
```

The backend reads credentials from `.env` via `app/config.py`
([ANGEL_API_KEY, ANGEL_CLIENT_CODE, ANGEL_PASSWORD, ANGEL_TOTP_SECRET](backend/.env.example)).
It logs in to Angel One lazily on the first API call (not at startup), and
re-logs in automatically if the session expires. All SmartAPI calls are bounded
by a semaphore and retried with backoff to stay within Angel One's rate limits.

On first use, `/api/symbols` downloads Angel One's instrument master and caches
the filtered NSE-equity list to `backend/data/nse_equity_instruments.json`
(refreshed daily).

## Frontend setup

```bash
cd frontend
npm install
cp .env.example .env           # points VITE_API_BASE_URL at the backend
npm run dev
```

Open http://localhost:5173, search for a symbol (e.g. `RELIANCE`, `TCS`,
`INFY`), and the dashboard will show its live quote (auto-refreshing every 15s)
plus a candlestick chart.

## API endpoints

| Endpoint | Description |
| --- | --- |
| `GET /api/symbols?q=` | Search NSE equities by ticker or company name |
| `GET /api/quote/{symbol}` | Live LTP/open/high/low/close for a symbol |
| `GET /api/historical/{symbol}?interval=ONE_DAY&days=90` | OHLCV candles |
