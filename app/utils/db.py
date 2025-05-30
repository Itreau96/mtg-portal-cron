import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import ijson
import contextlib
from ..config import settings

@contextlib.contextmanager
def get_db_connection():
    conn = psycopg2.connect(settings.db_url)
    try:
        yield conn
    finally:
        conn.close()

def truncate_table(cur, table_name: str):
    """
    Truncates a table if it exists (safe identifier handling).
    """
    cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = %s AND table_schema = 'public'
        )
    """, (table_name,))
    exists = cur.fetchone()[0]

    if exists:
        query = sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table_name))
        cur.execute(query)

def stream_insert_cards(cur, json_path, batch_size=1000):
    import json
    with open(json_path, 'r', encoding='utf-8') as f:
        cards_iter = ijson.items(f, 'item')
        batch = []
        for card in cards_iter:
            batch.append((
                card.get('id'),
                card.get('object'),
                card.get('oracle_id'),
                card.get('multiverse_ids'),
                card.get('mtgo_id'),
                card.get('arena_id'),
                card.get('tcgplayer_id'),
                card.get('name'),
                card.get('lang'),
                card.get('released_at'),
                card.get('uri'),
                card.get('scryfall_uri'),
                card.get('layout'),
                card.get('highres_image'),
                card.get('image_status'),
                json.dumps(card.get('image_uris')) if card.get('image_uris') else None,
                card.get('mana_cost'),
                card.get('cmc'),
                card.get('type_line'),
                card.get('oracle_text'),
                card.get('colors'),
                card.get('color_identity'),
                card.get('keywords'),
                card.get('produced_mana'),
                json.dumps(card.get('legalities')) if card.get('legalities') else None,
                card.get('games'),
                card.get('reserved'),
                card.get('game_changer'),
                card.get('foil'),
                card.get('nonfoil'),
                card.get('finishes'),
                card.get('oversized'),
                card.get('promo'),
                card.get('reprint'),
                card.get('variation'),
                card.get('set_id'),
                card.get('set'),
                card.get('set_name'),
                card.get('set_type'),
                card.get('set_uri'),
                card.get('set_search_uri'),
                card.get('scryfall_set_uri'),
                card.get('rulings_uri'),
                card.get('prints_search_uri'),
                card.get('collector_number'),
                card.get('digital'),
                card.get('rarity'),
                card.get('card_back_id'),
                card.get('artist'),
                card.get('artist_ids'),
                card.get('illustration_id'),
                card.get('border_color'),
                card.get('frame'),
                card.get('full_art'),
                card.get('textless'),
                card.get('booster'),
                card.get('story_spotlight'),
                json.dumps(card.get('prices')) if card.get('prices') else None,
                json.dumps(card.get('related_uris')) if card.get('related_uris') else None,
                json.dumps(card.get('purchase_uris')) if card.get('purchase_uris') else None,
                card.get('power'),
                card.get('toughness')
            ))
            if len(batch) >= batch_size:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {settings.staging_card_table} (
                        id, object, oracle_id, multiverse_ids, mtgo_id, arena_id, tcgplayer_id, name, lang, released_at, uri, scryfall_uri, layout, highres_image, image_status, image_uris, mana_cost, cmc, type_line, oracle_text, colors, color_identity, keywords, produced_mana, legalities, games, reserved, game_changer, foil, nonfoil, finishes, oversized, promo, reprint, variation, set_id, set, set_name, set_type, set_uri, set_search_uri, scryfall_set_uri, rulings_uri, prints_search_uri, collector_number, digital, rarity, card_back_id, artist, artist_ids, illustration_id, border_color, frame, full_art, textless, booster, story_spotlight, prices, related_uris, purchase_uris, power, toughness
                    ) VALUES %s ON CONFLICT (id) DO NOTHING
                    """,
                    batch
                )
                batch.clear()
        # Insert any remaining cards
        if batch:
            execute_values(
                cur,
                f"""
                INSERT INTO {settings.staging_card_table} (
                    id, object, oracle_id, multiverse_ids, mtgo_id, arena_id, tcgplayer_id, name, lang, released_at, uri, scryfall_uri, layout, highres_image, image_status, image_uris, mana_cost, cmc, type_line, oracle_text, colors, color_identity, keywords, produced_mana, legalities, games, reserved, game_changer, foil, nonfoil, finishes, oversized, promo, reprint, variation, set_id, set, set_name, set_type, set_uri, set_search_uri, scryfall_set_uri, rulings_uri, prints_search_uri, collector_number, digital, rarity, card_back_id, artist, artist_ids, illustration_id, border_color, frame, full_art, textless, booster, story_spotlight, prices, related_uris, purchase_uris, power, toughness
                ) VALUES %s ON CONFLICT (id) DO NOTHING
                """,
                batch
            )

def create_card_table(cur, table_name: str):
    """
    Creates a PostgreSQL table for CardDict structure.
    Nested fields are stored as JSONB.
    """

    query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {} (
        id TEXT PRIMARY KEY,
        object TEXT,
        oracle_id TEXT,
        multiverse_ids INTEGER[],
        mtgo_id INTEGER,
        arena_id INTEGER,
        tcgplayer_id INTEGER,
        name TEXT,
        lang TEXT,
        released_at DATE,
        uri TEXT,
        scryfall_uri TEXT,
        layout TEXT,
        highres_image BOOLEAN,
        image_status TEXT,
        image_uris JSONB,
        mana_cost TEXT,
        cmc FLOAT,
        type_line TEXT,
        oracle_text TEXT,
        colors TEXT[],
        color_identity TEXT[],
        keywords TEXT[],
        produced_mana TEXT[],
        legalities JSONB,
        games TEXT[],
        reserved BOOLEAN,
        game_changer BOOLEAN,
        foil BOOLEAN,
        nonfoil BOOLEAN,
        finishes TEXT[],
        oversized BOOLEAN,
        promo BOOLEAN,
        reprint BOOLEAN,
        variation BOOLEAN,
        set_id TEXT,
        set TEXT,
        set_name TEXT,
        set_type TEXT,
        set_uri TEXT,
        set_search_uri TEXT,
        scryfall_set_uri TEXT,
        rulings_uri TEXT,
        prints_search_uri TEXT,
        collector_number TEXT,
        digital BOOLEAN,
        rarity TEXT,
        card_back_id TEXT,
        artist TEXT,
        artist_ids TEXT[],
        illustration_id TEXT,
        border_color TEXT,
        frame TEXT,
        full_art BOOLEAN,
        textless BOOLEAN,
        booster BOOLEAN,
        story_spotlight BOOLEAN,
        prices JSONB,
        related_uris JSONB,
        purchase_uris JSONB,
        uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        power TEXT,
        toughness TEXT
    );
    """).format(sql.Identifier(table_name))

    cur.execute(query)

def swap_table_names(cur, table1: str, table2: str):
    """
    Atomically swaps the names of two tables in PostgreSQL.
    Uses a temporary name to avoid conflicts.
    """
    temp_name = f"{table1}_swap_temp"
    cur.execute(f'ALTER TABLE {table1} RENAME TO {temp_name};')
    cur.execute(f'ALTER TABLE {table2} RENAME TO {table1};')
    cur.execute(f'ALTER TABLE {temp_name} RENAME TO {table2};')