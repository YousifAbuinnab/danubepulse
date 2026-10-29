"""Application configuration."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "sqlite:///./danubepulse.db"
    bronze_dir: Path = Path("./data/bronze")
    lakehouse_root: Path = Path("./data/lakehouse")
    data_mode: Literal["demo", "real"] = "demo"
    entsoe_api_token: str | None = None
    entsoe_terms_accepted: bool = False
    demo_control_token: str | None = None
    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
