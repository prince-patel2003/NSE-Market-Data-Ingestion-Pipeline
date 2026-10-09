"""PostgreSQL access for the pipeline (asyncpg). All writes to OHLCV data go
through the stored functions defined in db/sql/03_functions.sql."""

import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import asyncpg

from app.config import Settings

logger = logging.getLogger("pipeline.db")

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
# 00_create_database.sql uses psql's \gexec; init_database() does that step itself.
SCHEMA_FILES = ["01_schema.sql", "02_trading_calendar.sql", "03_functions.sql"]
PIPELINE_LOCK_KEY = 0x4E5345  # pg advisory lock id: only one ingestion run at a time


@dataclass(frozen=True)
class InstrumentRow:
    instrument_id: int
    symbol_token: str
    symbol: str
    exchange: str


@dataclass(frozen=True)
class Gap:
    start: date
    end: date
    missing_days: int


@dataclass(frozen=True)
class BatchResult:
    inserted: int
    updated: int
    unchanged: int
    quarantined: int


def _connect_kwargs(settings: Settings, database: str | None = None) -> dict:
    return {
        "host": settings.db_host,
        "port": settings.db_port,
        "user": settings.db_user_name,
        "password": settings.db_password,
        "database": database or settings.db_name,
    }


async def init_database(settings: Settings) -> None:
    """Create the database if needed, then apply every schema file (all idempotent)."""
    conn = await asyncpg.connect(**_connect_kwargs(settings, database="postgres"))
    try:
        exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", settings.db_name)
        if not exists:
            logger.info("Creating database %s", settings.db_name)
            await conn.execute(f'CREATE DATABASE "{settings.db_name}" ENCODING \'UTF8\'')
    finally:
        await conn.close()

    conn = await asyncpg.connect(**_connect_kwargs(settings))
    try:
        for name in SCHEMA_FILES:
            logger.info("Applying %s", name)
            async with conn.transaction():
                await conn.execute((SQL_DIR / name).read_text(encoding="utf-8"))
    finally:
        await conn.close()


class Repository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._lock_conn: asyncpg.Connection | None = None

    @classmethod
    async def connect(cls, settings: Settings, max_size: int = 10) -> "Repository":
        pool = await asyncpg.create_pool(**_connect_kwargs(settings), min_size=1, max_size=max_size)
        return cls(pool)

    async def close(self) -> None:
        await self.release_pipeline_lock()
        await self._pool.close()

    # ------------------------------------------------------------------ run lock

    async def try_acquire_pipeline_lock(self) -> bool:
        """Session-level advisory lock held for the whole run on a dedicated connection."""
        self._lock_conn = await self._pool.acquire()
        acquired = await self._lock_conn.fetchval("SELECT pg_try_advisory_lock($1)", PIPELINE_LOCK_KEY)
        if not acquired:
            await self._pool.release(self._lock_conn)
            self._lock_conn = None
        return bool(acquired)

    async def release_pipeline_lock(self) -> None:
        if self._lock_conn is not None:
            await self._lock_conn.execute("SELECT pg_advisory_unlock($1)", PIPELINE_LOCK_KEY)
            await self._pool.release(self._lock_conn)
            self._lock_conn = None

    # ------------------------------------------------------------------ instruments

    async def instrument_count(self) -> int:
        return await self._pool.fetchval("SELECT count(*) FROM market.instruments")

    async def upsert_instruments(self, rows: list[tuple[str, str, str, str]]) -> int:
        """rows: (symbol_token, symbol, name, exchange)."""
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "CREATE TEMP TABLE _instr (symbol_token text, symbol text, name text, exchange text) ON COMMIT DROP"
            )
            await conn.copy_records_to_table("_instr", records=rows)
            status = await conn.execute(
                """
                INSERT INTO market.instruments (symbol_token, symbol, name, exchange)
                SELECT DISTINCT ON (symbol_token) symbol_token, symbol, name, exchange FROM _instr
                ON CONFLICT (symbol_token) DO UPDATE
                    SET symbol = EXCLUDED.symbol, name = EXCLUDED.name, exchange = EXCLUDED.exchange,
                        updated_at = now()
                    WHERE (market.instruments.symbol, market.instruments.name, market.instruments.exchange)
                          IS DISTINCT FROM (EXCLUDED.symbol, EXCLUDED.name, EXCLUDED.exchange)
                """
            )
        return int(status.split()[-1])

    async def find_instruments(self, symbols: list[str]) -> tuple[list[InstrumentRow], list[str]]:
        """Resolve tickers like 'RELIANCE' or 'RELIANCE-EQ'. Returns (found, unknown)."""
        wanted = {s.strip().upper(): s for s in symbols if s.strip()}
        keys = [k if k.endswith("-EQ") else f"{k}-EQ" for k in wanted]
        records = await self._pool.fetch(
            """
            SELECT DISTINCT ON (upper(symbol)) instrument_id, symbol_token, symbol, exchange
            FROM market.instruments
            WHERE exchange = 'NSE' AND upper(symbol) = ANY($1::text[])
            ORDER BY upper(symbol), updated_at DESC
            """,
            keys,
        )
        found = [InstrumentRow(**dict(r)) for r in records]
        found_keys = {r.symbol.upper() for r in found}
        unknown = [orig for key, orig in wanted.items()
                   if (key if key.endswith("-EQ") else f"{key}-EQ") not in found_keys]
        return found, unknown

    async def set_tracked(self, instrument_ids: list[int], tracked: bool) -> None:
        await self._pool.execute(
            "UPDATE market.instruments SET is_tracked = $2, updated_at = now() WHERE instrument_id = ANY($1::int[])",
            instrument_ids, tracked,
        )

    async def tracked_instruments(self) -> list[InstrumentRow]:
        records = await self._pool.fetch(
            "SELECT instrument_id, symbol_token, symbol, exchange FROM market.instruments "
            "WHERE is_tracked ORDER BY symbol"
        )
        return [InstrumentRow(**dict(r)) for r in records]

    # ------------------------------------------------------------------ runs

    async def start_run(self, from_date: date, to_date: date, instruments_total: int, params: dict) -> int:
        return await self._pool.fetchval(
            """
            INSERT INTO market.ingestion_runs (from_date, to_date, instruments_total, params)
            VALUES ($1, $2, $3, $4::jsonb) RETURNING run_id
            """,
            from_date, to_date, instruments_total, json.dumps(params),
        )

    async def finish_run(self, run_id: int, status: str, error: str | None = None) -> None:
        await self._pool.execute(
            """
            UPDATE market.ingestion_runs r
               SET finished_at        = now(),
                   status             = $2,
                   error              = $3,
                   instruments_failed = s.failed,
                   rows_fetched       = s.fetched,
                   rows_inserted      = s.inserted,
                   rows_updated       = s.updated,
                   rows_unchanged     = s.unchanged,
                   rows_quarantined   = s.quarantined
              FROM (SELECT count(*) FILTER (WHERE status = 'failed') AS failed,
                           coalesce(sum(rows_fetched), 0)     AS fetched,
                           coalesce(sum(rows_inserted), 0)    AS inserted,
                           coalesce(sum(rows_updated), 0)     AS updated,
                           coalesce(sum(rows_unchanged), 0)   AS unchanged,
                           coalesce(sum(rows_quarantined), 0) AS quarantined
                      FROM market.ingestion_run_items WHERE run_id = $1) s
             WHERE r.run_id = $1
            """,
            run_id, status, error,
        )

    async def start_item(self, run_id: int, instrument_id: int) -> None:
        await self._pool.execute(
            "INSERT INTO market.ingestion_run_items (run_id, instrument_id) VALUES ($1, $2) "
            "ON CONFLICT (run_id, instrument_id) DO NOTHING",
            run_id, instrument_id,
        )

    async def finish_item(self, run_id: int, instrument_id: int, status: str, stats: dict, error: str | None) -> None:
        await self._pool.execute(
            """
            UPDATE market.ingestion_run_items
               SET status = $3, gap_ranges = $4, missing_days = $5, rows_fetched = $6,
                   rows_inserted = $7, rows_updated = $8, rows_unchanged = $9,
                   rows_quarantined = $10, no_data_days = $11, error = $12, finished_at = now()
             WHERE run_id = $1 AND instrument_id = $2
            """,
            run_id, instrument_id, status,
            stats["gap_ranges"], stats["missing_days"], stats["rows_fetched"], stats["inserted"],
            stats["updated"], stats["unchanged"], stats["quarantined"], stats["no_data_days"], error,
        )

    # ------------------------------------------------------------------ stored functions

    async def detect_gaps(self, instrument_id: int, from_date: date, to_date: date) -> list[Gap]:
        records = await self._pool.fetch(
            "SELECT gap_start, gap_end, missing_days FROM market.detect_gaps($1, $2, $3)",
            instrument_id, from_date, to_date,
        )
        return [Gap(r["gap_start"], r["gap_end"], r["missing_days"]) for r in records]

    async def upsert_batch(self, run_id: int, instrument_id: int, payload: list[dict]) -> BatchResult:
        record = await self._pool.fetchrow(
            "SELECT * FROM market.upsert_ohlcv_batch($1, $2, $3::jsonb)",
            run_id, instrument_id, json.dumps(payload),
        )
        return BatchResult(**dict(record))

    async def record_fetch_result(self, run_id: int, instrument_id: int, from_date: date, to_date: date) -> int:
        return await self._pool.fetchval(
            "SELECT market.record_fetch_result($1, $2, $3, $4)", run_id, instrument_id, from_date, to_date
        )

    async def reprocess_quarantine(self, run_id: int) -> BatchResult:
        record = await self._pool.fetchrow("SELECT * FROM market.reprocess_quarantine($1)", run_id)
        return BatchResult(**dict(record))

    # ------------------------------------------------------------------ reporting

    async def status_summary(self) -> dict:
        async with self._pool.acquire() as conn:
            totals = await conn.fetchrow(
                """
                SELECT (SELECT count(*) FROM market.instruments)                 AS instruments,
                       (SELECT count(*) FROM market.instruments WHERE is_tracked) AS tracked,
                       (SELECT count(*) FROM market.ohlcv_daily)                 AS candles,
                       (SELECT min(trade_date) FROM market.ohlcv_daily)          AS first_date,
                       (SELECT max(trade_date) FROM market.ohlcv_daily)          AS last_date,
                       (SELECT count(*) FROM market.ohlcv_quarantine WHERE resolved_at IS NULL) AS quarantined
                """
            )
            runs = await conn.fetch(
                """
                SELECT run_id, started_at, finished_at, status, instruments_total, instruments_failed,
                       rows_fetched, rows_inserted, rows_updated, rows_quarantined
                FROM market.ingestion_runs ORDER BY run_id DESC LIMIT 5
                """
            )
        return {"totals": dict(totals), "runs": [dict(r) for r in runs]}
