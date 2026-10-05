from datetime import datetime

from pydantic import BaseModel


class SymbolResult(BaseModel):
    symbol: str
    name: str
    token: str
    exchange: str


class Quote(BaseModel):
    symbol: str
    exchange: str
    ltp: float
    open: float
    high: float
    low: float
    close: float
    change: float
    change_percent: float


class Candle(BaseModel):
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


class HistoricalResponse(BaseModel):
    symbol: str
    interval: str
    candles: list[Candle]
