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

# for title.akas
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

# for movies.basics
def create_table_movies_basics(cursor):
    # Create the table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS basics (
            tconst TEXT PRIMARY KEY,
            titleType TEXT,
            primaryTitle TEXT,
            originalTitle TEXT,
            isAdult INTEGER,
            startYear INTEGER,
            endYear INTEGER,
            runtimeMinutes INTEGER,
            genres TEXT
        )
    ''')

def ingest_movies_basics(conn, cursor, file_path):
    for row in lazy_pandas_csv_reader(file_path):
        upsert_query = '''
            INSERT INTO basics (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tconst) DO UPDATE SET
                titleType=excluded.titleType,
                primaryTitle=excluded.primaryTitle,
                originalTitle=excluded.originalTitle,
                isAdult=excluded.isAdult,
                startYear=excluded.startYear,
                endYear=excluded.endYear,
                runtimeMinutes=excluded.runtimeMinutes,
                genres=excluded.genres
        '''
        cursor.execute(upsert_query, row)
        # Optionally, commit periodically to avoid holding too many uncommitted rows in memory
        if cursor.lastrowid % 1000 == 0:
            conn.commit()
            # never call conn.close() from here!

def load_movies_basics(file_path: str):
    DATABASE_NAME = "movies.db"
    conn = sqlite3.connect(f"./processed_data/{DATABASE_NAME}")
    cursor = conn.cursor()
    create_table_movies_basics(cursor)
    conn.commit()
    ingest_movies_basics(conn, cursor, file_path)
    # Commit the transaction
    conn.commit()
    # Close the connection
    conn.close()

# for countries data
def create_table_countries(cursor):
    # Create the table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS countries (
            country TEXT PRIMARY KEY,
            abbreviation TEXT,
            region TEXT,
            population INTEGER,
            area_sq_mi REAL,
            pop_density_per_sq_mi REAL,
            coastline_ratio REAL,
            net_migration REAL,
            infant_mortality_per_1000_births REAL,
            gdp_capita REAL,
            literacy REAL,
            phones_per_1000 REAL,
            arable REAL,
            crops REAL,
            other REAL,
            climate REAL,
            birthrate REAL,
            deathrate REAL,
            agriculture REAL,
            industry REAL,
            service REAL
        )
    ''')

def ingest_countries_data(conn, cursor, file_path):
    for row in lazy_pandas_csv_reader(file_path):
        upsert_query = '''
            INSERT INTO countries (country, abbreviation, region, population, area_sq_mi, pop_density_per_sq_mi, coastline_ratio, net_migration, infant_mortality_per_1000_births, gdp_capita, literacy, phones_per_1000, arable, crops, other, climate, birthrate, deathrate, agriculture, industry, service)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(country) DO UPDATE SET
                abbreviation=excluded.abbreviation,
                region=excluded.region,
                population=excluded.population,
                area_sq_mi=excluded.area_sq_mi,
                pop_density_per_sq_mi=excluded.pop_density_per_sq_mi,
                coastline_ratio=excluded.coastline_ratio,
                net_migration=excluded.net_migration,
                infant_mortality_per_1000_births=excluded.infant_mortality_per_1000_births,
                gdp_capita=excluded.gdp_capita,
                literacy=excluded.literacy,
                phones_per_1000=excluded.phones_per_1000,
                arable=excluded.arable,
                crops=excluded.crops,
                other=excluded.other,
                climate=excluded.climate,
                birthrate=excluded.birthrate,
                deathrate=excluded.deathrate,
                agriculture=excluded.agriculture,
                industry=excluded.industry,
                service=excluded.service
        '''
        cursor.execute(upsert_query, row)
        # Optionally, commit periodically to avoid holding too many uncommitted rows in memory
        if cursor.lastrowid % 1000 == 0:
            conn.commit()
            # never call conn.close() from here!

def load_countries_data(file_path: str):
    conn = sqlite3.connect(f"./processed_data/{DATABASE_NAME}")
    cursor = conn.cursor()
    create_table_countries(cursor)
    conn.commit()
    ingest_countries_data(conn, cursor, file_path)
    # Commit the transaction
    conn.commit()
    # Close the connection
    conn.close()

# for crew data
def create_table_movies_crew(cursor):
    # Create the table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS crew (
            tconst TEXT PRIMARY KEY,
            directors TEXT,
            writers TEXT
        )
    ''')

def ingest_movies_crew(conn, cursor, file_path):
    for row in lazy_pandas_csv_reader(file_path):
        upsert_query = '''
            INSERT INTO crew (tconst, directors, writers)
            VALUES (?, ?, ?)
            ON CONFLICT(tconst) DO UPDATE SET
                directors=excluded.directors,
                writers=excluded.writers
        '''
        cursor.execute(upsert_query, row)
        # Optionally, commit periodically to avoid holding too many uncommitted rows in memory
        if cursor.lastrowid % 1000 == 0:
            conn.commit()
            # never call conn.close() from here!

def load_movies_crew(file_path: str):
    DATABASE_NAME = "movies.db"
    conn = sqlite3.connect(f"./processed_data/{DATABASE_NAME}")
    cursor = conn.cursor()
    create_table_movies_crew(cursor)
    conn.commit()
    ingest_movies_crew(conn, cursor, file_path)
    # Commit the transaction
    conn.commit()
    # Close the connection
    conn.close()

# for principals
def create_table_movie_principals(cursor):
    # Create the table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS principals (
            tconst TEXT,
            ordering INTEGER,
            nconst TEXT,
            category TEXT,
            job TEXT,
            characters TEXT,
            PRIMARY KEY (tconst, ordering, nconst)
        )
    ''')

def ingest_movie_principals(conn, cursor, file_path):
    for row in lazy_pandas_csv_reader(file_path):
        upsert_query = '''
            INSERT INTO principals (tconst, ordering, nconst, category, job, characters)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(tconst, ordering, nconst) DO UPDATE SET
                category=excluded.category,
                job=excluded.job,
                characters=excluded.characters
        '''
        cursor.execute(upsert_query, row)
        # Optionally, commit periodically to avoid holding too many uncommitted rows in memory
        if cursor.lastrowid % 1000 == 0:
            conn.commit()
            # never call conn.close() from here!

def load_movie_principals(file_path: str):
    DATABASE_NAME = "movies.db"
    conn = sqlite3.connect(f"./processed_data/{DATABASE_NAME}")
    cursor = conn.cursor()
    create_table_movie_principals(cursor)
    conn.commit()
    ingest_movie_principals(conn, cursor, file_path)
    # Commit the transaction
    conn.commit()
    # Close the connection
    conn.close()

# for ratings
def create_table_movie_ratings(cursor):
    # Create the table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ratings (
            tconst TEXT PRIMARY KEY,
            averageRating REAL,
            numVotes INTEGER
        )
    ''')

def ingest_movie_ratings(conn, cursor, file_path):
    for row in lazy_pandas_csv_reader(file_path):
        upsert_query = '''
            INSERT INTO ratings (tconst, averageRating, numVotes)
            VALUES (?, ?, ?)
            ON CONFLICT(tconst) DO UPDATE SET
                averageRating=excluded.averageRating,
                numVotes=excluded.numVotes
        '''
        cursor.execute(upsert_query, row)
        # Optionally, commit periodically to avoid holding too many uncommitted rows in memory
        if cursor.lastrowid % 1000 == 0:
            conn.commit()
            # never call conn.close() from here!

def load_movie_ratings(file_path: str):
    DATABASE_NAME = "movies.db"
    conn = sqlite3.connect(f"./processed_data/{DATABASE_NAME}")
    cursor = conn.cursor()
    create_table_movie_ratings(cursor)
    conn.commit()
    ingest_movie_ratings(conn, cursor, file_path)
    # Commit the transaction
    conn.commit()
    # Close the connection
    conn.close()

# for name basics
def create_table_name_basics(cursor):
    # Create the table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS name_basics (
            nconst TEXT PRIMARY KEY,
            primaryName TEXT,
            birthYear INTEGER,
            deathYear INTEGER,
            primaryProfession TEXT,
            knownForTitles TEXT
        )
    ''')

def ingest_name_basics(conn, cursor, file_path):
    for row in lazy_pandas_csv_reader(file_path):
        upsert_query = '''
            INSERT INTO name_basics (nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(nconst) DO UPDATE SET
                primaryName=excluded.primaryName,
                birthYear=excluded.birthYear,
                deathYear=excluded.deathYear,
                primaryProfession=excluded.primaryProfession,
                knownForTitles=excluded.knownForTitles
        '''
        cursor.execute(upsert_query, row)
        # Optionally, commit periodically to avoid holding too many uncommitted rows in memory
        if cursor.lastrowid % 1000 == 0:
            conn.commit()
            # never call conn.close() from here!

def load_name_basics(file_path: str):
    DATABASE_NAME = "movies.db"
    conn = sqlite3.connect(f"./processed_data/{DATABASE_NAME}")
    cursor = conn.cursor()
    create_table_name_basics(cursor)
    conn.commit()
    ingest_name_basics(conn, cursor, file_path)
    # Commit the transaction
    conn.commit()
    # Close the connection
    conn.close()


# for episodes
def create_table_episodes(cursor):
    # Create the table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS episodes (
            tconst TEXT PRIMARY KEY,
            parentTconst TEXT,
            seasonNumber INTEGER,
            episodeNumber INTEGER
        )
    ''')

def ingest_episodes(conn, cursor, file_path):
    for row in lazy_pandas_csv_reader(file_path):
        upsert_query = '''
            INSERT INTO episodes (tconst, parentTconst, seasonNumber, episodeNumber)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(tconst) DO UPDATE SET
                parentTconst=excluded.parentTconst,
                seasonNumber=excluded.seasonNumber,
                episodeNumber=excluded.episodeNumber
        '''
        cursor.execute(upsert_query, row)
        # Optionally, commit periodically to avoid holding too many uncommitted rows in memory
        if cursor.lastrowid % 1000 == 0:
            conn.commit()
            # never call conn.close() from here!

def load_episodes(file_path: str):
    DATABASE_NAME = "movies.db"
    conn = sqlite3.connect(f"./processed_data/{DATABASE_NAME}")
    cursor = conn.cursor()
    create_table_episodes(cursor)
    conn.commit()
    ingest_episodes(conn, cursor, file_path)
    # Commit the transaction
    conn.commit()
    # Close the connection
    conn.close()