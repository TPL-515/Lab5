import sqlite3
from pathlib import Path
import pandas as pd
# Get our sql session
db = Path('database.db')
con = sqlite3.connect(db)
cursor = con.cursor()

def create_table():
    create_table_text = """CREATE TABLE IF NOT EXISTS demo (
        id integer,
        name text NOT NULL,
        email text, 
        ingest_date text NOT NULL
    )"""

    try:
        cursor.execute(create_table_text)
    except:
        raise Exception

def get_table_meta():
    cursor.execute("SELECT id, name, email, ingest_date FROM demo")
    dat = cursor.fetchall()
    df = pd.DataFrame(dat, columns=['id', 'email', 'name', 'ingest_date'])
    rows = df.shape[0]
    cols = df.shape[1]
    return rows, cols

def add_data(data):
    cursor.executemany("INSERT INTO demo VALUES(?, ?, ?, ?)", data)
    con.commit()
    con.close()

def remove_data(tablename):
    cursor.execute(f'DELETE FROM {tablename}')
    con.commit()
    con.close()
