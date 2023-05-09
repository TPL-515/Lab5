from dagster import asset, get_dagster_logger
import sqlite3
from pathlib import Path
from datetime import datetime
# Get our Logger
logger = get_dagster_logger()

# Get our sql session
db = Path('database.db')
con = sqlite3.connect(db)
cursor = con.cursor()


@asset(description="This checks if our table exists")
def create_table():

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
        raise Exception

@asset(description="Return metadata for the database")
def display_db_meta(create_table):
    logger.info('Getting the meta data for our database')
    
    cursor.execute("SELECT id, name, email, ingest_date FROM demo")
    dat = cursor.fetchall()

    logger.info(f'Total number of entries in the table {len(dat)}')
    print(dat)

@asset(description="This ingests an example bit of data into the database")
def add_data(create_table):
    data = [(1, 'Joe Random', 'jrandom@mac.com', datetime.now())]
    
    logger.info(f'Injesting {len(data)} rows into the database')
    
    cursor.executemany("INSERT INTO demo VALUES(?, ?, ?, ?)", data)
    con.commit()
    con.close()

@asset(description="This allows the user to delete all the data from the database")
def remove_data():
    logger.info('Deleting all data from the database.')
    cursor.execute('DELETE FROM demo')
    con.commit()
    con.close()



        

