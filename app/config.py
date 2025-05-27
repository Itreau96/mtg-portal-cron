import logging
import sys
from pydantic_settings import BaseSettings

# Configure logging for the application
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""
    db_url: str
    live_card_table: str
    staging_card_table: str
    download_dir: str
    scryfall_url: str = "https://api.scryfall.com/bulk-data/default-cards"

    class Config:
        env_file = ".env"

# Instantiate settings after logging is configured
settings = Settings()