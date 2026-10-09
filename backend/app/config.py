from datetime import date
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


class Settings(BaseSettings):
    angel_api_key: str
    angel_client_code: str
    angel_password: str
    angel_totp_secret: str

    cors_origins: str = "http://localhost:5173"

    # PostgreSQL (used by the OHLCV ingestion pipeline in backend/db)
    db_host: str = "localhost"
    db_port: int = 5432
    db_user_name: str = "postgres"
    db_password: str = ""
    db_name: str = "nse_data"

    # Ingestion pipeline tuning
    ingest_max_concurrency: int = 3          # bounded semaphore size for in-flight Angel One requests
    ingest_requests_per_second: float = 3.0  # getCandleData is limited to 3 req/s by Angel One
    ingest_max_retries: int = 5
    ingest_backfill_start: date = date(2020, 1, 1)

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_prefix="", case_sensitive=False, extra="ignore")

    @property
    def cors_origin_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
