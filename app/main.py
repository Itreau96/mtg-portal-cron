from app.jobs.build_mtg_db import build_db
import logging

if __name__ == "__main__":
    logging.info(
    '''
    ___  ________ _____  ______          _        _   _____                 
    |  \/  |_   _|  __ \ | ___ \        | |      | | /  __ \                
    | .  . | | | | |  \/ | |_/ /__  _ __| |_ __ _| | | /  \/_ __ ___  _ __  
    | |\/| | | | | | __  |  __/ _ \| '__| __/ _` | | | |   | '__/ _ \| '_ \ 
    | |  | | | | | |_\ \ | | | (_) | |  | || (_| | | | \__/\ | | (_) | | | |
    \_|  |_/ \_/  \____/ \_|  \___/|_|   \__\__,_|_|  \____/_|  \___/|_| |_|
    MTG Portal Cron Job
    ''')
    logging.info("Starting cron jobs...")
    build_db()
    logging.info("Cron jobs completed successfully.")