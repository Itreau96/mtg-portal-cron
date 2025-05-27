from datetime import datetime
import logging
from pathlib import Path
from zipfile import ZipFile
import requests
from app.config import settings

def get_bulk_uri() -> str:
    """
    Returns the Scryfall bulk data URI.
    Returns:
        str: The URI for the default card Scryfall bulk data.
    """
    logging.info("Fetching Scryfall bulk data URI...")
    url = settings.scryfall_url
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    download_url = data["download_uri"] if "download_uri" in data else data["data"][0]["download_uri"]
    return download_url


def download_bulk_card_data(download_url) -> str:
    """
    Downloads bulk card data from Scryfall to a local file.
    Args:
        url (str): The URL to fetch the bulk card data from.
    Returns:
        str: local path where the bulk card data is saved.
    """
    logging.info("Fetching bulk card data...")
    # Setup download directory
    download_dir = Path(settings.download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_path = download_dir / f"scryfall_bulk_cards_{timestamp}.json"

    # Download the bulk data file
    r = requests.get(download_url, stream=True)
    r.raise_for_status()
    with open(local_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

    logging.info(f"Bulk card data downloaded to {local_path}")

    # If the file is a zip, extract it
    if str(local_path).endswith(".zip"):
        with ZipFile(local_path, 'r') as zip_ref:
            zip_ref.extractall(download_dir)
        # Find the JSON file inside the zip
        json_files = list(download_dir.glob("*.json"))
        if not json_files:
            raise FileNotFoundError("No JSON file found in the zip archive.")
        json_path = json_files[0]
    else:
        json_path = local_path
    
    return json_path