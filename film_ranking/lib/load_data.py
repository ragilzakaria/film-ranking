import csv
import sqlite3
import sys
import pandas as pd

DATABASE_NAME = "movies.db"
csv.field_size_limit(sys.maxsize)


def lazy_pandas_csv_reader(file_path, chunksize=1000):
    for chunk in pd.read_csv(file_path, chunksize=chunksize, delimiter='\t'):
        for row in chunk.itertuples(index=False, name=None):
            yield row


def create_table_movies_akas(cursor):
    # Create the table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS titles (
            titleId TEXT PRIMARY KEY,
            ordering INTEGER,
            title TEXT,
            region TEXT,
            language TEXT,
            types TEXT,
            attributes TEXT,
            isOriginalTitle BOOLEAN
        )
    ''')


def ingest_movies_akas(conn, cursor, file_path):
    for row in lazy_pandas_csv_reader(file_path):
        upsert_query = '''
            INSERT INTO titles (titleId, ordering, title, region, language, types, attributes, isOriginalTitle)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(titleId) DO UPDATE SET
                ordering=excluded.ordering,
                title=excluded.title,
                region=excluded.region,
                language=excluded.language,
                types=excluded.types,
                attributes=excluded.attributes,
                isOriginalTitle=excluded.isOriginalTitle
        '''
        cursor.execute(upsert_query, row)
        # Optionally, commit periodically to avoid holding too many uncommitted rows in memory
        if cursor.lastrowid % 1000 == 0:
            conn.commit()
            # never call conn.close() from here!


def load_movies_akas(file_path: str):
    conn = sqlite3.connect(f"./processed_data/{DATABASE_NAME}")
    cursor = conn.cursor()
    create_table_movies_akas(cursor)
    conn.commit()
    ingest_movies_akas(conn, cursor, file_path)
    # Commit the transaction

    # Close the connection
    conn.close()

