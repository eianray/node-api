from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # App
    app_name: str = "Meridian GIS API"
    app_version: str = "0.4.0"
    debug: bool = False

    # Database (operations log only — no accounts)
    database_url: str = "postgresql://meridian:meridian@localhost:5432/meridian"

    # x402 / USDC payments
    # Set to your Base wallet address to enable real payment verification.
    # Leave as zero address for dev mode (verification bypassed).
    wallet_address: str = "0x0000000000000000000000000000000000000000"
    x402_facilitator_url: str = "https://x402.org/facilitate"

    # File limits
    max_upload_mb: int = 50

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
