import logging
from app.utils.scryfall import get_bulk_uri, download_bulk_card_data
from app.utils.db import get_db_connection, create_card_table, stream_insert_cards, swap_table_names
from app.config import settings

def build_db():
    '''
    Builds the MTG database by fetching and processing bulk card data.
    This function retrieves the bulk data URI, downloads the card data,
    creates the table if needed, and streams the data into the database.
    '''
    logging.info("Starting MTG database build process...")
    bulk_uri = get_bulk_uri()
    data_path = download_bulk_card_data(bulk_uri)

    # Create the table if it doesn't exist and load data
    with get_db_connection() as conn:
        create_card_table(conn, settings.staging_card_table)
        logging.info(f"Created or verified table: {settings.staging_card_table}")
        stream_insert_cards(conn, data_path)
        swap_table_names(conn, settings.staging_card_table, settings.live_card_table)
        logging.info("Bulk card data loaded into database.")

    print("MTG database build complete.")