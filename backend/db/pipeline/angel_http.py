"""Async Angel One SmartAPI client built directly on httpx.

Only the endpoints the ingestion pipeline needs: login, historical candles and the
scrip master. Every request goes through a bounded semaphore (max in-flight
requests) plus a start-time throttle (max requests per second), and transient
failures are retried with exponential backoff and jitter.
"""

import asyncio
import logging
import random
import re
import socket
import time
import uuid
from datetime import date, timedelta

import httpx
import pyotp

from app.config import Settings

logger = logging.getLogger("pipeline.angel")

BASE_URL = "https://apiconnect.angelone.in"
LOGIN_PATH = "/rest/auth/angelbroking/user/v1/loginByPassword"
CANDLE_PATH = "/rest/secure/angelbroking/historical/v1/getCandleData"
SCRIP_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

# Angel One caps a single ONE_DAY getCandleData request at 2000 days.
MAX_DAYS_PER_REQUEST = 2000

# Error codes that mean the session token is no longer valid -> log in again.
TOKEN_ERROR_CODES = {"AG8001", "AG8002", "AG8003", "AB1010", "AB1011"}
# Error codes Angel One returns for transient server-side problems.
RETRYABLE_ERROR_CODES = {"AB1004", "AB2001"}
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

BACKOFF_BASE_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 30.0


class AngelApiError(RuntimeError):
    """Raised when a request fails permanently or exhausts its retries."""


class AngelAuthError(AngelApiError):
    """Login was rejected (wrong MPIN/TOTP/API key). Never retried: Angel One locks
    the account after a few wrong MPIN attempts."""


class _RetryableError(Exception):
    def __init__(self, message: str, retry_after: float | None = None, relogin: bool = False):
        super().__init__(message)
        self.retry_after = retry_after
        self.relogin = relogin


class RequestThrottle:
    """Spaces request start times so we never exceed ``rate`` requests per second."""

    def __init__(self, rate: float) -> None:
        self._interval = 1.0 / rate if rate > 0 else 0.0
        self._next_slot = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            delay = self._next_slot - now
            self._next_slot = max(now, self._next_slot) + self._interval
        if delay > 0:
            await asyncio.sleep(delay)


def _local_ip() -> str:
    try:
        return socket.gethostbyname(socket.gethostname())
    except OSError:
        return "127.0.0.1"


class AngelHistoricalClient:
    def __init__(
        self,
        settings: Settings,
        *,
        max_concurrency: int | None = None,
        requests_per_second: float | None = None,
        max_retries: int | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._settings = settings
        self._semaphore = asyncio.BoundedSemaphore(max_concurrency or settings.ingest_max_concurrency)
        self._throttle = RequestThrottle(requests_per_second or settings.ingest_requests_per_second)
        self._max_retries = max_retries or settings.ingest_max_retries
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None
        self._jwt: str | None = None
        self._session_generation = 0
        self._login_lock = asyncio.Lock()
        self._login_error: AngelAuthError | None = None

    async def __aenter__(self) -> "AngelHistoricalClient":
        mac = ":".join(re.findall("..", f"{uuid.getnode():012x}"))
        self._client = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=self._timeout,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-UserType": "USER",
                "X-SourceID": "WEB",
                "X-ClientLocalIP": _local_ip(),
                "X-ClientPublicIP": _local_ip(),
                "X-MACAddress": mac,
                "X-PrivateKey": self._settings.angel_api_key,
            },
        )
        return self

    async def __aexit__(self, *exc) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------ session

    async def login(self) -> None:
        """Log in eagerly so bad credentials fail the run once, before any fetching."""
        await self._login(self._session_generation)

    async def _login(self, seen_generation: int) -> None:
        async with self._login_lock:
            if self._login_error is not None:
                raise self._login_error
            if self._jwt is not None and self._session_generation != seen_generation:
                return  # another task already refreshed the session
            logger.info("Logging in to Angel One SmartAPI")
            payload = {
                "clientcode": self._settings.angel_client_code,
                "password": self._settings.angel_password,
                "totp": pyotp.TOTP(self._settings.angel_totp_secret).now(),
            }
            try:
                body = await self._request(LOGIN_PATH, payload, authenticated=False)
                token = (body.get("data") or {}).get("jwtToken")
                if not token:
                    raise AngelApiError(f"login returned no token: {body.get('message')}")
            except AngelApiError as exc:
                self._login_error = AngelAuthError(f"Angel One login failed: {exc}")
                raise self._login_error from exc
            self._jwt = token
            self._session_generation += 1

    # ------------------------------------------------------------------ requests

    async def _send_once(self, path: str, payload: dict, authenticated: bool) -> dict:
        assert self._client is not None, "use 'async with AngelHistoricalClient(...)'"
        headers = {"Authorization": f"Bearer {self._jwt}"} if authenticated else None

        async with self._semaphore:
            await self._throttle.wait()
            try:
                response = await self._client.post(path, json=payload, headers=headers)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                raise _RetryableError(f"{type(exc).__name__}: {exc}") from exc

        text = response.text
        if response.status_code in RETRYABLE_STATUS or "exceeding access rate" in text.lower():
            retry_after = response.headers.get("Retry-After")
            raise _RetryableError(
                f"HTTP {response.status_code}: {text[:200]}",
                retry_after=float(retry_after) if retry_after and retry_after.isdigit() else None,
            )
        if response.status_code in (401, 403) and authenticated:
            raise _RetryableError(f"HTTP {response.status_code}: {text[:200]}", relogin=True)
        if response.status_code >= 400:
            raise AngelApiError(f"HTTP {response.status_code}: {text[:200]}")

        try:
            body = response.json()
        except ValueError as exc:
            raise _RetryableError(f"Non-JSON response: {text[:200]}") from exc

        if not body.get("status", False):
            code = body.get("errorcode") or body.get("errorCode") or ""
            message = f"{code} {body.get('message', 'Unknown Angel One error')}".strip()
            if code in TOKEN_ERROR_CODES and authenticated:
                raise _RetryableError(message, relogin=True)
            if code in RETRYABLE_ERROR_CODES:
                raise _RetryableError(message)
            raise AngelApiError(message)
        return body

    async def _request(self, path: str, payload: dict, authenticated: bool = True) -> dict:
        last_error: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            if authenticated and self._jwt is None:
                await self._login(self._session_generation)
            generation = self._session_generation
            try:
                return await self._send_once(path, payload, authenticated)
            except _RetryableError as exc:
                last_error = exc
                if attempt == self._max_retries:
                    break
                delay = exc.retry_after or min(BACKOFF_MAX_SECONDS, BACKOFF_BASE_SECONDS * 2 ** (attempt - 1))
                delay += random.uniform(0, delay / 2)
                logger.warning(
                    "%s failed (attempt %d/%d): %s - retrying in %.1fs",
                    path, attempt, self._max_retries, exc, delay,
                )
                if exc.relogin:
                    self._jwt = None if generation == self._session_generation else self._jwt
                await asyncio.sleep(delay)
        raise AngelApiError(f"{path} failed after {self._max_retries} attempts: {last_error}") from last_error

    # ------------------------------------------------------------------ public API

    async def get_daily_candles(self, exchange: str, symbol_token: str, from_date: date, to_date: date) -> list[list]:
        """Daily candles for [from_date, to_date], split into <=2000-day requests."""
        rows: list[list] = []
        window_start = from_date
        while window_start <= to_date:
            window_end = min(to_date, window_start + timedelta(days=MAX_DAYS_PER_REQUEST - 1))
            body = await self._request(
                CANDLE_PATH,
                {
                    "exchange": exchange,
                    "symboltoken": symbol_token,
                    "interval": "ONE_DAY",
                    "fromdate": f"{window_start:%Y-%m-%d} 09:00",
                    "todate": f"{window_end:%Y-%m-%d} 15:30",
                },
            )
            rows.extend(body.get("data") or [])
            window_start = window_end + timedelta(days=1)
        return rows


async def fetch_scrip_master(max_retries: int = 3) -> list[dict]:
    """Download Angel One's full instrument master (public, no auth)."""
    last_error: Exception | None = None
    async with httpx.AsyncClient(timeout=120.0) as client:
        for attempt in range(1, max_retries + 1):
            try:
                response = await client.get(SCRIP_MASTER_URL)
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                logger.warning("Scrip master download failed (attempt %d/%d): %s", attempt, max_retries, exc)
                await asyncio.sleep(BACKOFF_BASE_SECONDS * 2 ** attempt)
    raise AngelApiError(f"Could not download scrip master: {last_error}") from last_error
