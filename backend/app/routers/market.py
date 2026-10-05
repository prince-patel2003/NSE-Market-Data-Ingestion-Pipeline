from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query

from ..angel_client import AngelOneError, get_angel_client
from ..instruments import get_instrument_store
from ..schemas import Candle, HistoricalResponse, Quote, SymbolResult

router = APIRouter(prefix="/api", tags=["market"])

ALLOWED_INTERVALS = {
    "ONE_MINUTE",
    "FIVE_MINUTE",
    "FIFTEEN_MINUTE",
    "THIRTY_MINUTE",
    "ONE_HOUR",
    "ONE_DAY",
}


@router.get("/symbols", response_model=list[SymbolResult])
async def search_symbols(q: str = Query(..., min_length=1, description="Ticker or company name fragment")):
    store = get_instrument_store()
    matches = await store.search(q)
    return [
        SymbolResult(symbol=inst.symbol.removesuffix("-EQ"), name=inst.name, token=inst.token, exchange=inst.exch_seg)
        for inst in matches
    ]


@router.get("/quote/{symbol}", response_model=Quote)
async def get_quote(symbol: str):
    store = get_instrument_store()
    instrument = await store.get(symbol)
    if instrument is None:
        raise HTTPException(status_code=404, detail=f"Unknown NSE symbol '{symbol}'")

    client = get_angel_client()
    try:
        data = await client.ltp_data(instrument.exch_seg, instrument.symbol, instrument.token)
    except AngelOneError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    close = float(data["close"])
    ltp = float(data["ltp"])
    change = ltp - close
    change_percent = (change / close * 100) if close else 0.0

    return Quote(
        symbol=instrument.symbol.removesuffix("-EQ"),
        exchange=instrument.exch_seg,
        ltp=ltp,
        open=float(data["open"]),
        high=float(data["high"]),
        low=float(data["low"]),
        close=close,
        change=round(change, 2),
        change_percent=round(change_percent, 2),
    )


@router.get("/historical/{symbol}", response_model=HistoricalResponse)
async def get_historical(
    symbol: str,
    interval: str = Query("ONE_DAY", description="One of " + ", ".join(sorted(ALLOWED_INTERVALS))),
    days: int = Query(90, ge=1, le=365, description="How many calendar days of history to fetch"),
):
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(status_code=400, detail=f"interval must be one of {sorted(ALLOWED_INTERVALS)}")

    store = get_instrument_store()
    instrument = await store.get(symbol)
    if instrument is None:
        raise HTTPException(status_code=404, detail=f"Unknown NSE symbol '{symbol}'")

    to_date = datetime.now()
    from_date = to_date - timedelta(days=days)
    params = {
        "exchange": instrument.exch_seg,
        "symboltoken": instrument.token,
        "interval": interval,
        "fromdate": from_date.strftime("%Y-%m-%d %H:%M"),
        "todate": to_date.strftime("%Y-%m-%d %H:%M"),
    }

    client = get_angel_client()
    try:
        rows = await client.candle_data(params)
    except AngelOneError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    candles = [
        Candle(time=row[0], open=row[1], high=row[2], low=row[3], close=row[4], volume=row[5])
        for row in rows
    ]

    return HistoricalResponse(symbol=instrument.symbol.removesuffix("-EQ"), interval=interval, candles=candles)
