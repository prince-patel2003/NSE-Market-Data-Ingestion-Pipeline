"""Ingestion orchestration: gap detection -> fetch -> normalise -> validated upsert."""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone

from app.config import Settings

from .angel_http import AngelApiError, AngelHistoricalClient, fetch_scrip_master
from .repository import Gap, InstrumentRow, Repository
from .transform import candles_to_frame, frame_to_payload

logger = logging.getLogger("pipeline.ingest")

IST = timezone(timedelta(hours=5, minutes=30))
# The day's candle is final once the session (and closing auction) is over.
DAILY_CANDLE_FINAL_AFTER = time(16, 0)
# Instruments processed in parallel. Each one's API calls still pass through the
# client's bounded semaphore and rate throttle, so this only bounds DB work in flight.
INSTRUMENT_WORKERS = 6


def last_completed_session_date(now: datetime | None = None) -> date:
    now = now or datetime.now(IST)
    return now.date() if now.time() >= DAILY_CANDLE_FINAL_AFTER else now.date() - timedelta(days=1)


@dataclass
class ItemStats:
    gap_ranges: int = 0
    missing_days: int = 0
    rows_fetched: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    quarantined: int = 0
    no_data_days: int = 0

    def as_dict(self) -> dict:
        return self.__dict__.copy()


@dataclass
class RunSummary:
    run_id: int
    status: str
    instruments: int
    failed: list[str] = field(default_factory=list)
    totals: ItemStats = field(default_factory=ItemStats)


async def sync_instruments(repo: Repository) -> int:
    """Load NSE equities (``*-EQ``) from Angel One's scrip master into market.instruments."""
    logger.info("Downloading Angel One scrip master...")
    full_list = await fetch_scrip_master()
    rows = [
        (str(item["token"]), item["symbol"], item.get("name") or item["symbol"], "NSE")
        for item in full_list
        if item.get("exch_seg") == "NSE" and str(item.get("symbol", "")).endswith("-EQ")
    ]
    changed = await repo.upsert_instruments(rows)
    logger.info("Scrip master: %d NSE equities, %d new/changed", len(rows), changed)
    return changed


class IngestionPipeline:
    def __init__(self, settings: Settings, repo: Repository, client: AngelHistoricalClient) -> None:
        self._settings = settings
        self._repo = repo
        self._client = client

    async def run(
        self,
        instruments: list[InstrumentRow],
        from_date: date,
        to_date: date,
        recheck_days: int = 0,
    ) -> RunSummary:
        await self._client.login()  # fail fast on bad credentials, before recording a run
        run_id = await self._repo.start_run(
            from_date, to_date, len(instruments),
            {"symbols": [i.symbol for i in instruments], "recheck_days": recheck_days},
        )
        logger.info("Run %d: %d instruments, %s -> %s", run_id, len(instruments), from_date, to_date)
        summary = RunSummary(run_id=run_id, status="running", instruments=len(instruments))

        workers = asyncio.Semaphore(INSTRUMENT_WORKERS)

        async def worker(inst: InstrumentRow) -> None:
            async with workers:
                stats, error = await self._ingest_instrument(run_id, inst, from_date, to_date, recheck_days)
            for key, value in stats.as_dict().items():
                setattr(summary.totals, key, getattr(summary.totals, key) + value)
            if error:
                summary.failed.append(inst.symbol)

        try:
            await asyncio.gather(*(worker(inst) for inst in instruments))
        except BaseException as exc:  # includes Ctrl+C / cancellation
            await self._repo.finish_run(run_id, "failed", f"{type(exc).__name__}: {exc}")
            raise

        if not summary.failed:
            summary.status = "success"
        elif len(summary.failed) < len(instruments):
            summary.status = "partial"
        else:
            summary.status = "failed"
        await self._repo.finish_run(
            run_id, summary.status,
            f"failed: {', '.join(sorted(summary.failed))}" if summary.failed else None,
        )
        return summary

    async def _ingest_instrument(
        self, run_id: int, inst: InstrumentRow, from_date: date, to_date: date, recheck_days: int
    ) -> tuple[ItemStats, str | None]:
        stats = ItemStats()
        await self._repo.start_item(run_id, inst.instrument_id)
        try:
            gaps = await self._repo.detect_gaps(inst.instrument_id, from_date, to_date)
            stats.gap_ranges = len(gaps)
            stats.missing_days = sum(g.missing_days for g in gaps)

            ranges = [(g.start, g.end, True) for g in gaps]
            if recheck_days > 0:
                # Re-fetch the most recent days even if stored, to pick up corrections.
                recheck_from = max(from_date, to_date - timedelta(days=recheck_days))
                ranges.append((recheck_from, to_date, False))

            for start, end, is_gap in ranges:
                await self._fill_range(run_id, inst, start, end, is_gap, stats)

            status = "success" if ranges else "up_to_date"
            logger.info(
                "%-16s gaps=%d missing=%d fetched=%d +%d ~%d =%d quarantined=%d no_data=%d",
                inst.symbol, stats.gap_ranges, stats.missing_days, stats.rows_fetched, stats.inserted,
                stats.updated, stats.unchanged, stats.quarantined, stats.no_data_days,
            )
            await self._repo.finish_item(run_id, inst.instrument_id, status, stats.as_dict(), None)
            return stats, None
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            if isinstance(exc, AngelApiError):
                logger.error("%s: %s", inst.symbol, error)
            else:
                logger.exception("%s: unexpected error", inst.symbol)
            await self._repo.finish_item(run_id, inst.instrument_id, "failed", stats.as_dict(), error)
            return stats, error

    async def _fill_range(
        self, run_id: int, inst: InstrumentRow, start: date, end: date, is_gap: bool, stats: ItemStats
    ) -> None:
        rows = await self._client.get_daily_candles(inst.exchange, inst.symbol_token, start, end)
        stats.rows_fetched += len(rows)
        if rows:
            payload = frame_to_payload(candles_to_frame(rows))
            result = await self._repo.upsert_batch(run_id, inst.instrument_id, payload)
            stats.inserted += result.inserted
            stats.updated += result.updated
            stats.unchanged += result.unchanged
            stats.quarantined += result.quarantined
        if is_gap:
            stats.no_data_days += await self._repo.record_fetch_result(run_id, inst.instrument_id, start, end)


def describe_gaps(gaps: list[Gap]) -> str:
    return ", ".join(f"{g.start}..{g.end} ({g.missing_days}d)" for g in gaps) or "none"
