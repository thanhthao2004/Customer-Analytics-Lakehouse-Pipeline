"""Shared settings loaded from .env"""

from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).parent.parent / ".env")


class Settings:
    # PostgreSQL
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", 5432))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "customer_analytics")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "analytics")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "analytics123")

    @property
    def postgres_url(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def postgres_jdbc_url(self) -> str:
        return (
            f"jdbc:postgresql://{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # Paths
    RAW_DATA_DIR: str = os.getenv("RAW_DATA_DIR", "data/raw")
    PROCESSED_DATA_DIR: str = os.getenv("PROCESSED_DATA_DIR", "data/processed")
    LOGS_DIR: str = os.getenv("LOGS_DIR", "logs")

    # Scraping
    REQUEST_DELAY_SECONDS: float = float(os.getenv("REQUEST_DELAY_SECONDS", 2))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", 3))

    # Gemini API
    GE