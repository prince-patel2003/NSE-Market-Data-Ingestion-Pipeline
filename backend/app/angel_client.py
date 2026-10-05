import asyncio
import logging
import time

import pyotp
from SmartApi import SmartConnect

from .config import get_settings

logger = logging.getLogger("angel_client")

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1.5
SESSION_TTL_SECONDS = 6 * 60 * 60  # Angel One sessions are valid for a trading day; re-login periodically


class AngelOneError(RuntimeError):
    """Raised when the Angel One API returns an error or an unexpected response shape."""


class AngelOneClient:
    """Async-friendly wrapper around the (blocking) Angel One SmartAPI SDK.

    Calls are bounded by a semaphore so we don't blow through Angel One's per-second
    rate limits, and transient failures are retried with backoff.
    """

    def __init__(self) -> None:
        self._settings = get_settings()
        self._connect: SmartConnect | None = None
        self._session_created_at: float = 0.0
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(3)

    async def _ensure_session(self) -> SmartConnect:
        async with self._lock:
            session_expired = (time.monotonic() - self._session_created_at) > SESSION_TTL_SECONDS
            if self._connect is not None and not session_expired:
                return self._connect

            def _login() -> SmartConnect:
                connect = SmartConnect(api_key=self._settings.angel_api_key)
                totp = pyotp.TOTP(self._settings.angel_totp_secret).now()
                response = connect.generateSession(
                    self._settings.angel_client_code,
                    self._settings.angel_password,
                    totp,
                )
                if not response.get("status"):
                    raise AngelOneError(f"Angel One login failed: {response.get('message')}")
                return connect

            logger.info("Logging in to Angel One SmartAPI")
            self._connect = await asyncio.to_thread(_login)
            self._session_created_at = time.monotonic()
            return self._connect

    async def _call(self, fn_name: str, *args, **kwargs):
        last_error: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            connect = await self._ensure_session()
            async with self._semaphore:
                try:
                    fn = getattr(connect, fn_name)
                    result = await asyncio.to_thread(fn, *args, **kwargs)
                except Exception as exc:  # network error, SDK raising on HTTP failure, etc.
                    last_error = exc
                    logger.warning("Angel One call %s failed (attempt %d/%d): %s", fn_name, attempt, MAX_RETRIES, exc)
                else:
                    if isinstance(result, dict) and not result.get("status", True):
                        last_error = AngelOneError(result.get("message", "Unknown Angel One API error"))
                        logger.warning(
                            "Angel One call %s returned an error (attempt %d/%d): %s",
                            fn_name,
                            attempt,
                            MAX_RETRIES,
                            last_error,
                        )
                        # An invalid/expired token should force a fresh login on the next attempt.
                        if result.get("errorcode") in {"AG8001", "AG8002", "AG8003"}:
                            self._connect = None
                    else:
                        return result
            await asyncio.sleep(RETRY_BACKOFF_SECONDS * attempt)

        assert last_error is not None
        raise AngelOneError(str(last_error)) from last_error

    async def ltp_data(self, exchange: str, trading_symbol: str, symbol_token: str) -> dict:
        result = await self._call("ltpData", exchange, trading_symbol, symbol_token)
        return result["data"]

    async def candle_data(self, params: dict) -> list[list]:
        result = await self._call("getCandleData", params)
        return result["data"] or []


_client: AngelOneClient | None = None


def get_angel_client() -> AngelOneClient:
    global _client
    if _client is None:
        _client = AngelOneClient()
    return _client
