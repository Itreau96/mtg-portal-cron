import logging
from app.utils.scryfall import get_bulk_uri, download_bulk_card_data
from app.utils.db import get_db_connection, create_card_table, stream_insert_cards, swap_table_names, truncate_table
from app.config import settings

def build_db():
    '''
    Builds the MTG database by fetching and processing bulk card data.
    This function retrieves the bulk data URI, downloads the card data,
    creates the table if needed, and streams the data into the database.
    All DB operations are performed in a transaction. If any fail, the transaction is rolled back.
    '''
    logging.info("Starting MTG database build process...")
    bulk_uri = get_bulk_uri()
    data_path = download_bulk_card_data(bulk_uri)
    # data_path = './bulk_data/scryfall_bulk_cards_20250527_212626.json'  # For testing

    with get_db_connection() as conn:
        try:
            with conn:
                with conn.cursor() as cur:
                    logging.info("Performing table setup...")
                    truncate_table(cur, settings.staging_card_table)
                    create_card_table(cur, settings.staging_card_table)
                    create_card_table(cur, settings.live_card_table)
                    logging.info(f"Created or verified table: {settings.staging_card_table}")
                    logging.info(f"Loading data from {data_path} into staging table: {settings.staging_card_table}")
                    stream_insert_cards(cur, data_path)
                    swap_table_names(cur, settings.staging_card_table, settings.live_card_table)
                    logging.info("Bulk card data loaded into database.")
        except Exception as e:
            logging.error(f"Database transaction failed and was rolled back: {e}")
            conn.rollback()
            return

    logging.info("MTG database build complete.")