import csv
import sqlite3

DATABASE_NAME = "movies.db"


def create_table_movies_akas(cursor):
    # Create the table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS titles (
            titleId TEXT,
            ordering INTEGER,
            title TEXT,
            region TEXT,
            language TEXT,
            types TEXT,
            attributes TEXT,
            isOriginalTitle BOOLEAN
        )
    ''')


def ingest_movies_akas(cursor, file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter='\t')
        data = [(row['titleId'], row['ordering'], row['title'], row['region'], row['language'], row['types'],
                 row['attributes'], row['isOriginalTitle']) for row in reader]

        cursor.executemany('''
            INSERT INTO titles (titleId, ordering, title, region, language, types, attributes, isOriginalTitle)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', data)


def load_movies_akas(file_path: str):
    conn = sqlite3.connect(f"./processed_data/{DATABASE_NAME}")
    cursor = conn.cursor()
    create_table_movies_akas(cursor)
    ingest_movies_akas(cursor, file_path)
    # Commit the transaction
    conn.commit()
    # Close the connection
    conn.close()

