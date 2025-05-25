from pydantic import BaseSettings

class Settings(BaseSettings):
    db_url: str
    card_table: str
    download_dir: str
    scryfall_url: str = "https://api/scryfall.com/bulk-data/default-cards"

    class Config:
        env_file = ".env"

settings = Settings()