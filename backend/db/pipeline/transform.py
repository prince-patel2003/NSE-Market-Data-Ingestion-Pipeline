"""Pandas normalisation of raw Angel One candles into the stored-function payload.

This step only parses and normalises. Rows it cannot parse are *kept*, with NULL
fields, so that market.upsert_ohlcv_batch() quarantines them along with the raw
values. All validation rules live in the database, in one place.
"""

import math

import pandas as pd

COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]
PRICE_COLUMNS = ["open", "high", "low", "close"]


def _json_safe(value):
    # NaN/Infinity are not valid JSON; keep them visible in the quarantined raw row as text.
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return value


def candles_to_frame(rows: list[list]) -> pd.DataFrame:
    """Raw ``[[ts, o, h, l, c, v], ...]`` -> typed DataFrame with a ``trade_date`` column (IST)."""
    # Pad or trim ragged rows so a malformed candle can't break the whole batch.
    normalised = [(list(row) + [None] * len(COLUMNS))[: len(COLUMNS)] if isinstance(row, (list, tuple))
                  else [None] * len(COLUMNS) for row in rows]
    raw = pd.DataFrame(normalised, columns=COLUMNS, dtype=object)

    df = pd.DataFrame(index=raw.index)
    ts = pd.to_datetime(raw["timestamp"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")
    df["trade_date"] = ts.dt.date
    for col in PRICE_COLUMNS:
        df[col] = pd.to_numeric(raw[col], errors="coerce")
    df["volume"] = pd.to_numeric(raw["volume"], errors="coerce")
    df["raw"] = [{k: _json_safe(v) for k, v in zip(COLUMNS, row)} for row in normalised]

    # Exact duplicates (same day, same values) are harmless repeats -> keep one.
    # Conflicting duplicates are left in for the database to quarantine.
    df = df.drop_duplicates(subset=["trade_date", *PRICE_COLUMNS, "volume"], keep="last")
    return df.sort_values("trade_date", na_position="last").reset_index(drop=True)


def _clean(value):
    if value is None or value is pd.NaT or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, float) and math.isinf(value):
        return str(value).replace("inf", "Infinity")  # JSON has no infinity; the DB range check rejects it
    return value


def _volume(value):
    value = _clean(value)
    if value is None or isinstance(value, str):
        return value
    # Integral floats (e.g. 1234.0) become ints; anything fractional stays and is
    # rejected by the database's bigint check.
    return int(value) if float(value).is_integer() else float(value)


def frame_to_payload(df: pd.DataFrame) -> list[dict]:
    payload = []
    for rec in df.to_dict("records"):
        trade_date = _clean(rec["trade_date"])
        payload.append({
            "trade_date": trade_date.isoformat() if trade_date is not None else None,
            "open": _clean(rec["open"]),
            "high": _clean(rec["high"]),
            "low": _clean(rec["low"]),
            "close": _clean(rec["close"]),
            "volume": _volume(rec["volume"]),
            "source_row": rec["raw"],
        })
    return payload
