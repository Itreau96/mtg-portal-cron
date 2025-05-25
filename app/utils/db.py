import psycopg2
from ..models.card_dict import CardDict
from ..config import settings
from contextlib import contextmanager

def get_db_connection():
    conn = psycopg2.connect(settings.db_url)
    try:
        yield conn
    finally:
        conn.close()

def get_bulk_card_data(conn) -> list[CardDict]:
    print("Fetching bulk card data...")
    return []

def insert_bulk_card_data(conn, cards):
    with conn.cursor() as cursor:
        for card in cards:
            print(f"Inserting card: {card['name']}")