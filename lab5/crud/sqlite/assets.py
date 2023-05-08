from dagster import asset, get_dagster_logger
import sqlite3
from pathlib import Path
from datetime import datetime
# Get our Logger
logger = get_dagster_logger()

@asset(description="This asset checks if a database has been initialized and if not creates it.")
def initialize():
    logger.info('Checking if database exists')
    # Set our database name
    db = Path('database.db')
    # Check if the database exists
    if not db.exists():
        logger.warn('Database does not exist creating now')
    # get connection
    try:
        con = sqlite3.connect(db)
        logger.info('Succesfully connected to the database')
    except:
        logger.error('Failed to create or connect to database')
    return db

@asset(description="This checks if our table exists")
def create_table(initialize):
    logger.info('Getting the database connection')
    try:
        con = sqlite3.connect(initalize)
        cursor = con.cursor()
    except:
        logger.error('Could not connect to the database cursor.')

    logger.info('Create table if it does not exist within our database')

    create_table_text = """CREATE TABLE IF NOT EXISTS demo (
        id integer PRIMARY KEY,
        name text NOT NULL,
        email text, 
        ingest_date text NOT NULL
    )"""

    try:
        cursor.execute(create_table_text)
    except:
        logger.error('Had issues with creating the table in the database')

@asset(description="Return metadata for the database")
def display_db_meta(initialize):
    logger.info('Getting the meta data for our database')
    try:
        logger.info(intialize)
        con = sqlite3.connect(initalize)
        cursor = con.cursor()
    except:
        logger.error('Could not connect to the database cursor.')
    
    ids, names, emails, ingest_dates = cursor.execute("SELECT id, name, email ,ingest_date FROM demo ORDER BY name DESC")

    logger.info(f'Total number of entries in the table {len(ids)}')

@asset(description="This allows the user to pass in a list of data that then gets ingested into the database.")
def add_data(initialize):
    data = [(1, 'Joe Random', 'jrandom@mac.com', datetime.now().strftime('yyyy-MM-dd HH:mm:ss'))]
    try:
        logger.info(f"database is: {intialize}")
        con = sqlite3.connect(initalize)
        cursor = con.cursor()
    except:
        logger.error('Could not connect to the database cursor.')
    
    logger.info(f'Injesting {len(data)} rows into the database')
    try:
        cursor.executemany("INSERT INTO demo VALUES(?, ?, ?, ?)", data)
    except:
        logger.error('Failed to ingest data into the database.')

        

