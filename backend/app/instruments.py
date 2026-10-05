import asyncio
import json
import logging
import time
from pathlib import Path

import httpx

logger = logging.getLogger("instruments")

SCRIP_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
CACHE_PATH = Path(__file__).resolve().parent.parent / "data" / "nse_equity_instruments.json"
CACHE_TTL_SECONDS = 24 * 60 * 60


class Instrument:
    __slots__ = ("token", "symbol", "name", "exch_seg")

    def __init__(self, token: str, symbol: str, name: str, exch_seg: str):
        self.token = token
        self.symbol = symbol
        self.name = name
        self.exch_seg = exch_seg

    def to_dict(self) -> dict:
        return {"token": self.token, "symbol": self.symbol, "name": self.name, "exch_seg": self.exch_seg}


class InstrumentStore:
    """Downloads and caches Angel One's instrument master, filtered to NSE equities.

    Angel One's trading APIs key everything off a numeric ``symboltoken`` rather than
    the ticker, so we need this lookup before we can request a quote or candle data.
    """

    def __init__(self) -> None:
        self._instruments: list[Instrument] = []
        self._by_symbol: dict[str, Instrument] = {}
        self._lock = asyncio.Lock()

    async def _load(self) -> None:
        async with self._lock:
            if self._instruments:
                return

            if CACHE_PATH.exists() and (time.time() - CACHE_PATH.stat().st_mtime) < CACHE_TTL_SECONDS:
                logger.info("Loading NSE instrument list from cache at %s", CACHE_PATH)
                raw = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
            else:
                logger.info("Downloading Angel One scrip master (this can take a few seconds)...")
                async with httpx.AsyncClient(timeout=60.0) as client:
                    response = await client.get(SCRIP_MASTER_URL)
                    response.raise_for_status()
                    full_list = response.json()

                raw = [
                    {
                        "token": item["token"],
                        "symbol": item["symbol"],
                        "name": item["name"],
                        "exch_seg": item["exch_seg"],
                    }
                    for item in full_list
                    if item.get("exch_seg") == "NSE" and item.get("symbol", "").endswith("-EQ")
                ]
                CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
                CACHE_PATH.write_text(json.dumps(raw), encoding="utf-8")
                logger.info("Cached %d NSE equity instruments to %s", len(raw), CACHE_PATH)

            self._instruments = [Instrument(**item) for item in raw]
            self._by_symbol = {inst.symbol.upper(): inst for inst in self._instruments}
            # Also index by the bare name without the "-EQ" suffix for convenience.
            for inst in self._instruments:
                bare = inst.symbol.removesuffix("-EQ").upper()
                self._by_symbol.setdefault(bare, inst)

    async def search(self, query: str, limit: int = 15) -> list[Instrument]:
        await self._load()
        q = query.strip().upper()
        if not q:
            return []
        matches = [
            inst
            for inst in self._instruments
            if q in inst.symbol.upper() or q in inst.name.upper()
        ]
        matches.sort(key=lambda inst: (not inst.symbol.upper().startswith(q), len(inst.symbol)))
        return matches[:limit]

    async def get(self, symbol: str) -> Instrument | None:
        await self._load()
        key = symbol.strip().upper()
        if not key.endswith("-EQ"):
            direct = self._by_symbol.get(key)
            if direct:
                return direct
        return self._by_symbol.get(key)


_store: InstrumentStore | None = None


def get_instrument_store() -> InstrumentStore:
    global _store
    if _store is None:
        _store = InstrumentStore()
    return _store
