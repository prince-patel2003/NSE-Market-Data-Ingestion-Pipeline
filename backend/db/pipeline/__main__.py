"""Command-line entry point. Run from the backend/ directory:

    python -m db.pipeline init-db
    python -m db.pipeline sync-instruments
    python -m db.pipeline track --file db/watchlist.txt
    python -m db.pipeline run
    python -m db.pipeline gaps RELIANCE
    python -m db.pipeline status
"""

import argparse
import asyncio
import logging
import sys
from datetime import date
from pathlib import Path

from app.config import get_settings

from .angel_http import AngelAuthError, AngelHistoricalClient
from .ingest import IST, IngestionPipeline, describe_gaps, last_completed_session_date, sync_instruments
from .repository import Repository, init_database

logger = logging.getLogger("pipeline")


def _read_symbols(args: argparse.Namespace) -> list[str]:
    symbols = list(args.symbols or [])
    if getattr(args, "file", None):
        for line in Path(args.file).read_text(encoding="utf-8").splitlines():
            line = line.split("#", 1)[0].strip()
            if line:
                symbols.append(line)
    # Accept comma separated values too: "RELIANCE,TCS"
    return [s for chunk in symbols for s in chunk.split(",") if s.strip()]


async def _resolve(repo: Repository, symbols: list[str]):
    if await repo.instrument_count() == 0:
        await sync_instruments(repo)
    found, unknown = await repo.find_instruments(symbols)
    for sym in unknown:
        logger.warning("Unknown NSE symbol '%s' (skipped)", sym)
    return found


async def cmd_init_db(args, settings) -> int:
    await init_database(settings)
    logger.info("Database %s is ready", settings.db_name)
    return 0


async def cmd_sync_instruments(args, settings) -> int:
    repo = await Repository.connect(settings)
    try:
        await sync_instruments(repo)
    finally:
        await repo.close()
    return 0


async def cmd_track(args, settings, tracked: bool) -> int:
    symbols = _read_symbols(args)
    if not symbols:
        logger.error("Give symbols and/or --file")
        return 2
    repo = await Repository.connect(settings)
    try:
        found = await _resolve(repo, symbols)
        await repo.set_tracked([i.instrument_id for i in found], tracked)
        logger.info("%s %d instruments", "Tracking" if tracked else "Untracked", len(found))
    finally:
        await repo.close()
    return 0


async def cmd_run(args, settings) -> int:
    to_date = args.to_date or last_completed_session_date()
    from_date = args.from_date or settings.ingest_backfill_start
    if from_date > to_date:
        logger.error("--from (%s) is after --to (%s)", from_date, to_date)
        return 2

    repo = await Repository.connect(settings)
    try:
        if not await repo.try_acquire_pipeline_lock():
            logger.error("Another ingestion run is in progress (advisory lock held); exiting")
            return 3

        symbols = _read_symbols(args)
        instruments = await _resolve(repo, symbols) if symbols else await repo.tracked_instruments()
        if not instruments:
            logger.error("No instruments to ingest. Track some first: python -m db.pipeline track --file db/watchlist.txt")
            return 2

        try:
            async with AngelHistoricalClient(settings) as client:
                pipeline = IngestionPipeline(settings, repo, client)
                summary = await pipeline.run(instruments, from_date, to_date, recheck_days=args.recheck_days)
        except AngelAuthError as exc:
            logger.error("%s - fix ANGEL_* credentials in backend/.env before retrying "
                         "(repeated wrong MPINs lock the Angel One account)", exc)
            return 5

        t = summary.totals
        logger.info(
            "Run %d %s: %d instruments (%d failed), fetched=%d inserted=%d updated=%d unchanged=%d "
            "quarantined=%d no_data_days=%d",
            summary.run_id, summary.status.upper(), summary.instruments, len(summary.failed),
            t.rows_fetched, t.inserted, t.updated, t.unchanged, t.quarantined, t.no_data_days,
        )
        return {"success": 0, "partial": 1}.get(summary.status, 4)
    finally:
        await repo.close()


async def cmd_gaps(args, settings) -> int:
    to_date = args.to_date or last_completed_session_date()
    from_date = args.from_date or settings.ingest_backfill_start
    repo = await Repository.connect(settings)
    try:
        symbols = _read_symbols(args)
        instruments = await _resolve(repo, symbols) if symbols else await repo.tracked_instruments()
        for inst in instruments:
            gaps = await repo.detect_gaps(inst.instrument_id, from_date, to_date)
            print(f"{inst.symbol:<18} {describe_gaps(gaps)}")
    finally:
        await repo.close()
    return 0


async def cmd_reprocess(args, settings) -> int:
    repo = await Repository.connect(settings)
    try:
        today = date.today()
        run_id = await repo.start_run(today, today, 0, {"command": "reprocess-quarantine"})
        result = await repo.reprocess_quarantine(run_id)
        await repo.finish_run(run_id, "success")
        logger.info("Reprocessed quarantine (run %d): %s", run_id, result)
    finally:
        await repo.close()
    return 0


async def cmd_status(args, settings) -> int:
    repo = await Repository.connect(settings)
    try:
        summary = await repo.status_summary()
    finally:
        await repo.close()
    t = summary["totals"]
    print(f"Instruments: {t['instruments']} ({t['tracked']} tracked)")
    print(f"Candles:     {t['candles']} ({t['first_date']} .. {t['last_date']})")
    print(f"Quarantined: {t['quarantined']} unresolved rows")
    print("\nRecent runs:")
    for r in summary["runs"]:
        print(
            f"  #{r['run_id']:<5} {r['started_at'].astimezone(IST):%Y-%m-%d %H:%M} IST {r['status']:<8} "
            f"instruments={r['instruments_total']} failed={r['instruments_failed']} "
            f"fetched={r['rows_fetched']} inserted={r['rows_inserted']} updated={r['rows_updated']} "
            f"quarantined={r['rows_quarantined']}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m db.pipeline", description="NSE daily OHLCV ingestion pipeline")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Create the database, tables, calendar and stored functions")
    sub.add_parser("sync-instruments", help="Load/refresh NSE equities from Angel One's scrip master")

    for name, help_text in (("track", "Add symbols to the tracked set"), ("untrack", "Remove symbols")):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("symbols", nargs="*")
        p.add_argument("--file", help="Text file with one symbol per line")

    def add_range(p):
        p.add_argument("symbols", nargs="*", help="Symbols (default: all tracked)")
        p.add_argument("--file", help="Text file with one symbol per line")
        p.add_argument("--from", dest="from_date", type=date.fromisoformat, help="Start date YYYY-MM-DD")
        p.add_argument("--to", dest="to_date", type=date.fromisoformat, help="End date (default: last completed session)")

    run = sub.add_parser("run", help="Detect gaps and backfill daily OHLCV")
    add_range(run)
    run.add_argument("--recheck-days", type=int, default=0,
                     help="Also re-fetch the last N calendar days to pick up corrections")

    add_range(sub.add_parser("gaps", help="Show missing date ranges without fetching"))
    sub.add_parser("reprocess-quarantine", help="Re-validate quarantined rows (e.g. after a calendar fix)")
    sub.add_parser("status", help="Show table counts and recent runs")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    settings = get_settings()

    commands = {
        "init-db": cmd_init_db,
        "sync-instruments": cmd_sync_instruments,
        "track": lambda a, s: cmd_track(a, s, True),
        "untrack": lambda a, s: cmd_track(a, s, False),
        "run": cmd_run,
        "gaps": cmd_gaps,
        "reprocess-quarantine": cmd_reprocess,
        "status": cmd_status,
    }
    return asyncio.run(commands[args.command](args, settings))


if __name__ == "__main__":
    sys.exit(main())
