import sqlite3
import pandas as pd
from typing import Literal, Optional

CinematicRankSort = Literal["impact_score", "awards_count"]


def get_connection(file="./processed_data/film.db"):
    conn = sqlite3.connect(file)
    return conn


def search_movie(keyword: str, limit: int = 10):
    conn = get_connection()
    query = f"""
    SELECT b.tconst, b.originalTitle, b.startYear, GROUP_CONCAT(DISTINCT (nb.primaryName))
FROM basics b
JOIN principals p on p.tconst = b.tconst
JOIN name_basics nb on nb.nconst = p.nconst
WHERE originalTitle = "{keyword}"
GROUP BY b.tconst
LIMIT {limit};
    """

    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def search_person(keyword: str, limit: int = 10):
    conn = get_connection()
    query = f"""
    SELECT nb.nconst, nb.primaryProfession,
       nb.primaryName,
       GROUP_CONCAT(b.primaryTitle) titles,
       b.genres
FROM name_basics nb
JOIN basics b
    ON nb.knownForTitles = b.tconst
    OR SUBSTR(nb.knownForTitles, 1, INSTR(nb.knownForTitles, ',') - 1) = b.tconst
WHERE primaryName = "{keyword}"
GROUP BY nconst
LIMIT {limit};
    """

    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def get_movies_with_regional_data(yearStart: int, yearEnd: int, sort_by=None):
    conn = get_connection()

    query = f"""
    WITH ranked_regions AS (
    SELECT
        a.titleId,
        b.primaryTitle,
        a.region,
        a.ordering,
        ROW_NUMBER() OVER (PARTITION BY a.titleId ORDER BY a.ordering DESC) AS rank
    FROM
        akas a
    JOIN
        basics b ON a.titleId = b.tconst
    WHERE
        b.startYear >= {yearStart} AND b.startYear <= {yearEnd}
        AND LOWER(a.title) = LOWER(b.primaryTitle)
        AND a.isOriginalTitle = 0
        AND a.region != '\\N'
        AND LENGTH(a.region) < 3
    ),
    original_regions AS (
        SELECT
            titleId,
            primaryTitle,
            region
        FROM
            ranked_regions
        WHERE
            rank = 1
    ),
    country_cinema_data AS (
        SELECT
            o.region,
            countries.country AS countryName,
            countries.population,
            countries.gdp_capita,
            countries.gdp_capita * countries.population AS gdp,
            COUNT(o.titleId) AS numFilms,
            SUM(r.numVotes) AS votes,
            AVG(r.averageRating) AS avgRating,
            SUM(r.numVotes) * AVG(r.averageRating) AS qualityScore,
            CAST(countries.population / COUNT(o.titleId) AS FLOAT) AS populationPerFilm,
            CAST(COUNT(o.titleId) / (countries.gdp_capita * countries.population) AS FLOAT) AS filmPerGdp,
            CAST((countries.gdp_capita * countries.population) / COUNT(o.titleId) AS FLOAT) AS gdpPerFilm,
            CAST(COUNT(o.titleId) / countries.gdp_capita AS FLOAT) AS gdpCapitaPerFilm
        FROM
            original_regions o
        JOIN ratings r ON o.titleId = r.tconst
        JOIN countries ON countries.abbreviation = o.region
        GROUP BY
            o.region
    ),
    country_ranks AS (
        SELECT
            countryName,
            gdp,
            numFilms,
            qualityScore,
            ROW_NUMBER() OVER (ORDER BY gdp DESC) AS gdp_rank,
            ROW_NUMBER() OVER (ORDER BY numFilms DESC) AS weak_cinematic_impact_rank,
            ROW_NUMBER() OVER (ORDER BY qualityScore DESC) AS strong_cinematic_impact_rank
        FROM
            country_cinema_data
    )
    SELECT
        ccd.region AS region,
        ccd.countryName,
        ccd.population AS population,
        ccd.gdp_capita AS gdp_capita,
        ccd.gdp AS gdp,
        ccd.numFilms AS numFilms,
        ccd.votes AS votes,
        ccd.avgRating AS avgRating,
        ccd.qualityScore AS qualityScore,
        ccd.populationPerFilm,
        ccd.filmPerGdp,
        ccd.gdpPerFilm,
        ccd.gdpCapitaPerFilm,
        cr.gdp_rank,
        cr.weak_cinematic_impact_rank,
        cr.strong_cinematic_impact_rank,
        (cr.gdp_rank - cr.weak_cinematic_impact_rank) AS weak_cinematic_impact_difference,
        (cr.gdp_rank - cr.strong_cinematic_impact_rank) AS strong_cinematic_impact_difference
    FROM
        country_cinema_data ccd
    JOIN
        country_ranks cr ON ccd.countryName = cr.countryName
    ORDER BY {'"' + sort_by + '"' if sort_by else "qualityScore" } DESC;
    """

    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def get_cinematic_rank(
    year_start: int,
    year_end: int,
    limit=None,
    genre=None,
    mtype=None,
    country=None,
    sort_by: Optional[CinematicRankSort] = None,
):
    conn = get_connection()

    query = f"""
    WITH ranked_regions AS (
    SELECT
        a.titleId,
        b.primaryTitle,
        a.region,
        a.ordering,
        ROW_NUMBER() OVER (PARTITION BY a.titleId ORDER BY a.ordering DESC) AS rank
    FROM
        akas a
    JOIN
        basics b ON a.titleId = b.tconst
    WHERE
        b.startYear >= {year_start} AND b.startYear <= {year_end}
        {"AND b.genres =" + '"' + genre + '"' if genre else ""}
        {"AND b.titleType = " + '"' + mtype + '"' if mtype else ""}
        AND LOWER(a.title) = LOWER(b.primaryTitle)
        AND a.isOriginalTitle = 0
        AND a.region != '\\N'
        AND length(a.region) < 3
    ),
    original_regions AS (
        SELECT
            titleId,
            primaryTitle,
            region
        FROM
            ranked_regions
        WHERE
            rank = 1
    ),
    awards_concat AS (
        SELECT
            const,
            GROUP_CONCAT(awardName || ' ' || categoryName || ' ' || year, ', ') AS awards,
            COUNT(*) AS awards_count
        FROM
            awards
        GROUP BY
            const
    )
    SELECT
        o.titleId,
        o.primaryTitle,
        o.region,
        r.numVotes,
        r.averageRating,
        r.averageRating * r.numVotes AS product,
        a.awards,
        a.awards_count
    FROM
        original_regions o
    JOIN
        ratings r ON o.titleId = r.tconst
    LEFT JOIN
        awards_concat a ON o.titleId = a.const
    {"WHERE o.region = " + '"' + country + '"' if country else ""}
    ORDER BY {"awards_count" if sort_by == "awards_count" else "product" } DESC
    {"LIMIT " + str(limit) if limit else ""};
    """
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def get_directors_rank(yearStart: int, yearEnd: int, sort_by=None):
    conn = get_connection()

    query = f"""
    WITH ranked_regions AS (
    SELECT
        a.titleId,
        b.primaryTitle,
        a.region,
        a.ordering,
        ROW_NUMBER() OVER (PARTITION BY a.titleId ORDER BY a.ordering DESC) AS rank
    FROM
        akas a
    JOIN
        basics b ON a.titleId = b.tconst
    WHERE
        b.startYear >= {yearStart} AND b.startYear <= {yearEnd}
        AND LOWER(a.title) = LOWER(b.primaryTitle)
        AND a.isOriginalTitle = 0
        AND a.region != '\\N'
        AND LENGTH(a.region) < 3
    ),
    original_regions AS (
        SELECT
            titleId,
            primaryTitle,
            region
        FROM
            ranked_regions
        WHERE
            rank = 1
    ),
    awards_concat AS (
        SELECT
            const,
            GROUP_CONCAT(awardName || ' ' || categoryName || ' ' || year, ', ') AS awards,
            COUNT(*) AS awardsCount
        FROM
            awards
        GROUP BY
            const
    ),
    director_movies AS (
        SELECT
            p.nconst AS directorId,
            p.tconst AS movieId,
            b.primaryTitle,
            r.averageRating,
            r.numVotes,
            r.averageRating * r.numVotes AS product,
            COALESCE(a.awardsCount, 0) AS hasAwards
        FROM
            principals p
        JOIN
            basics b ON p.tconst = b.tconst
        JOIN
            ratings r ON p.tconst = r.tconst
        JOIN
            original_regions o ON p.tconst = o.titleId
        LEFT JOIN
            awards_concat a ON p.tconst = a.const
        WHERE
            p.category = 'director'
    ),
    director_aggregates AS (
        SELECT
            d.directorId,
            COUNT(d.movieId) AS movie_count,
            GROUP_CONCAT(d.primaryTitle) AS movies,
            AVG(d.averageRating) AS avgRating,
            SUM(d.numVotes) AS totalVotes,
            AVG(d.averageRating) * SUM(d.numVotes) AS totalProduct,
            SUM(CASE WHEN d.hasAwards > 0 THEN 1 ELSE 0 END) AS moviesWithAwards,
            SUM(CASE WHEN d.hasAwards = 0 THEN 1 ELSE 0 END) AS moviesWithoutAwards
        FROM
            director_movies d
        GROUP BY
            d.directorId
    )
    SELECT
        da.directorId,
        n.primaryName AS directorName,
        da.movies,
        da.movie_count AS movieCount,
        a.awards,
        a.awardsCount AS awardsCount,
        da.avgRating AS avgRating,
        da.totalVotes,
        da.totalProduct,
        da.moviesWithAwards AS moviesWithAwards,
        da.moviesWithoutAwards,
        ((3 * da.moviesWithAwards + 1 * da.moviesWithoutAwards)/da.movie_count) * da.avgRating *da.totalVotes AS finalAssessmentValue,
        (da.moviesWithAwards * 100 / da.movie_count) AS awardPercentage
    FROM
        director_aggregates da
    JOIN
        name_basics n ON da.directorId = n.nconst
    LEFT JOIN
        awards_concat a ON a.const = da.directorId
    ORDER BY {'"' + sort_by + '"' if sort_by else "finalAssessmentValue" } DESC;
"""
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def get_producers_rank(yearStart: int, yearEnd: int, sort_by=None):
    conn = get_connection()

    query = f"""
    WITH ranked_regions AS (
    SELECT
        a.titleId,
        b.primaryTitle,
        a.region,
        a.ordering,
        ROW_NUMBER() OVER (PARTITION BY a.titleId ORDER BY a.ordering DESC) AS rank
    FROM
        akas a
    JOIN
        basics b ON a.titleId = b.tconst
    WHERE
        b.startYear >= {yearStart} AND b.startYear <= {yearEnd}
        AND LOWER(a.title) = LOWER(b.primaryTitle)
        AND a.isOriginalTitle = 0
        AND a.region != '\\N'
        AND LENGTH(a.region) < 3
    ),
    original_regions AS (
        SELECT
            titleId,
            primaryTitle,
            region
        FROM
            ranked_regions
        WHERE
            rank = 1
    ),
    awards_concat AS (
        SELECT
            const,
            GROUP_CONCAT(awardName || ' ' || categoryName || ' ' || year, ', ') AS awards,
            COUNT(*) AS awardsCount
        FROM
            awards
        GROUP BY
            const
    ),
    producer_movies AS (
        SELECT
            p.nconst AS producerId,
            p.tconst AS movieId,
            b.primaryTitle,
            r.averageRating,
            r.numVotes,
            r.averageRating * r.numVotes AS product
        FROM
            principals p
        JOIN
            basics b ON p.tconst = b.tconst
        JOIN
            ratings r ON p.tconst = r.tconst
        JOIN
            original_regions o ON p.tconst = o.titleId
        WHERE
            p.category = 'producer'
    ),
    producer_aggregates AS (
        SELECT
            p.producerId,
            COUNT(p.movieId) AS movie_count,
            GROUP_CONCAT(p.primaryTitle) AS movies,
            AVG(p.averageRating) AS avgRating,
            SUM(p.numVotes) AS totalVotes,
            AVG(p.averageRating) * SUM(p.numVotes) AS totalProduct
        FROM
            producer_movies p
        GROUP BY
            p.producerId
    )
    SELECT
        pa.producerId,
        n.primaryName AS producerName,
        pa.movies AS movie,
        pa.movie_count AS movieCount,
        a.awards,
        a.awardsCount AS awardsCount,
        pa.avgRating AS avgRating,
        pa.totalVotes,
        pa.totalProduct AS totalProduct
    FROM
        producer_aggregates pa
    JOIN
        name_basics n ON pa.producerId = n.nconst
    LEFT JOIN
        awards_concat a ON a.const = pa.producerId
    ORDER BY {'"' + sort_by + '"' if sort_by else "totalProduct" } DESC;
"""
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def get_actors_rank(yearStart: int, yearEnd: int, sort_by=None):
    conn = get_connection()

    query = f"""
    WITH ranked_regions AS (
    SELECT
        a.titleId,
        b.primaryTitle,
        a.region,
        a.ordering,
        ROW_NUMBER() OVER (PARTITION BY a.titleId ORDER BY a.ordering DESC) AS rank
    FROM
        akas a
    JOIN
        basics b ON a.titleId = b.tconst
    WHERE
        b.startYear >= {yearStart} AND b.startYear <= {yearEnd}
        AND LOWER(a.title) = LOWER(b.primaryTitle)
        AND a.isOriginalTitle = 0
        AND a.region != '\\N'
        AND LENGTH(a.region) < 3
    ),
    original_regions AS (
        SELECT
            titleId,
            primaryTitle,
            region
        FROM
            ranked_regions
        WHERE
            rank = 1
    ),
    awards_concat AS (
        SELECT
            const,
            GROUP_CONCAT(awardName || ' ' || categoryName || ' ' || year, ', ') AS awards,
            COUNT(*) AS awardsCount
        FROM
            awards
        GROUP BY
            const
    ),
    actor_movies AS (
        SELECT
            p.nconst AS actorId,
            p.tconst AS movieId,
            b.primaryTitle,
            r.averageRating,
            r.numVotes,
            r.averageRating * r.numVotes AS product,
            o.region
        FROM
            principals p
        JOIN
            basics b ON p.tconst = b.tconst
        JOIN
            ratings r ON p.tconst = r.tconst
        JOIN
            original_regions o ON p.tconst = o.titleId
        WHERE
            p.category = 'actor' OR 'actress'
    ),
    actor_aggregates AS (
        SELECT
            p.actorId,
            COUNT(p.movieId) AS movieCount,
            GROUP_CONCAT(DISTINCT p.primaryTitle) AS movies,
            AVG(p.averageRating) AS avgRating,
            SUM(p.numVotes) AS totalVotes,
            AVG(p.averageRating) * SUM(p.numVotes) AS totalProduct,
            GROUP_CONCAT(DISTINCT p.region) AS regions,
            COUNT(DISTINCT p.region) AS countryCount
        FROM
            actor_movies p
        GROUP BY
            p.actorId
    )
        SELECT
            aa.actorId,
            n.primaryName AS actorName,
            aa.movies,
            aa.movieCount AS movieCount,
            a.awards,
            a.awardsCount AS awardsCount,
            aa.avgRating AS avgRating,
            aa.totalVotes,
            aa.totalProduct,
            aa.regions,
            aa.countryCount AS countryCount
        FROM
            actor_aggregates aa
        JOIN
            name_basics n ON aa.actorId = n.nconst
        LEFT JOIN
            awards_concat a ON a.const = aa.actorId
        ORDER BY {'"' + sort_by + '"' if sort_by else "awardsCount" } DESC;
"""
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def actors_comparison(actorId1: str, actorId2: str):
    conn = get_connection()

    query = f"""
    WITH ranked_regions AS (
    SELECT
        a.titleId,
        b.primaryTitle,
        a.region,
        a.ordering,
        ROW_NUMBER() OVER (PARTITION BY a.titleId ORDER BY a.ordering DESC) AS rank
    FROM
        akas a
    JOIN
        basics b ON a.titleId = b.tconst
    WHERE
        LOWER(a.title) = LOWER(b.primaryTitle)
        AND a.isOriginalTitle = 0
        AND a.region != '\\N'
        AND LENGTH(a.region) < 3
    ),
    original_regions AS (
        SELECT
            titleId,
            primaryTitle,
            region
        FROM
            ranked_regions
        WHERE
            rank = 1
    ),
    awards_concat AS (
        SELECT
            const,
            GROUP_CONCAT(awardName || ' ' || categoryName || ' ' || year, ', ') AS awards,
            COUNT(*) AS awardsCount
        FROM
            awards
        GROUP BY
            const
    ),
    actor_movies AS (
        SELECT
            p.nconst AS actorId,
            p.tconst AS movieId,
            b.primaryTitle,
            r.averageRating,
            r.numVotes,
            r.averageRating * r.numVotes AS product,
            o.region
        FROM
            principals p
        JOIN
            basics b ON p.tconst = b.tconst
        JOIN
            ratings r ON p.tconst = r.tconst
        JOIN
            original_regions o ON p.tconst = o.titleId
        WHERE
            p.category = 'actor' OR 'actress'
    ),
    actor_aggregates AS (
        SELECT
            p.actorId,
            COUNT(p.movieId) AS movieCount,
            GROUP_CONCAT(DISTINCT p.primaryTitle) AS movies,
            AVG(p.averageRating) AS avgRating,
            SUM(p.numVotes) AS totalVotes,
            AVG(p.averageRating) * SUM(p.numVotes) AS totalProduct,
            GROUP_CONCAT(DISTINCT p.region) AS regions,
            COUNT(DISTINCT p.region) AS countryCount
        FROM
            actor_movies p
        WHERE
            actorId IN ('{actorId1}', '{actorId2}')
        GROUP BY
            p.actorId
    )
        SELECT
            aa.actorId,
            n.primaryName AS actorName,
            aa.movies,
            aa.movieCount,
            a.awards,
            a.awardsCount,
            aa.avgRating,
            aa.totalVotes,
            aa.totalProduct,
            aa.regions,
            aa.countryCount
        FROM
            actor_aggregates aa
        JOIN
            name_basics n ON aa.actorId = n.nconst
        LEFT JOIN
            awards_concat a ON a.const = aa.actorId
    """
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def directors_comparison(directorId1: str, directorId2: str):
    conn = get_connection()

    query = f"""
    WITH ranked_regions AS (
    SELECT
        a.titleId,
        b.primaryTitle,
        a.region,
        a.ordering,
        ROW_NUMBER() OVER (PARTITION BY a.titleId ORDER BY a.ordering DESC) AS rank
    FROM
        akas a
    JOIN
        basics b ON a.titleId = b.tconst
    WHERE
        LOWER(a.title) = LOWER(b.primaryTitle)
        AND a.isOriginalTitle = 0
        AND a.region != '\\N'
        AND LENGTH(a.region) < 3
    ),
    original_regions AS (
        SELECT
            titleId,
            primaryTitle,
            region
        FROM
            ranked_regions
        WHERE
            rank = 1
    ),
    awards_concat AS (
        SELECT
            const,
            GROUP_CONCAT(awardName || ' ' || categoryName || ' ' || year, ', ') AS awards,
            COUNT(*) AS awardsCount
        FROM
            awards
        GROUP BY
            const
    ),
    director_movies AS (
        SELECT
            p.nconst AS directorId,
            p.tconst AS movieId,
            b.primaryTitle,
            r.averageRating,
            r.numVotes,
            r.averageRating * r.numVotes AS product,
            COALESCE(a.awardsCount, 0) AS hasAwards
        FROM
            principals p
        JOIN
            basics b ON p.tconst = b.tconst
        JOIN
            ratings r ON p.tconst = r.tconst
        JOIN
            original_regions o ON p.tconst = o.titleId
        LEFT JOIN
            awards_concat a ON p.tconst = a.const
        WHERE
            p.category = 'director'
            AND p.nconst IN ('{directorId1}', '{directorId2}')
    ),
    director_aggregates AS (
        SELECT
            d.directorId,
            COUNT(d.movieId) AS movie_count,
            GROUP_CONCAT(d.primaryTitle) AS movies,
            AVG(d.averageRating) AS avgRating,
            SUM(d.numVotes) AS totalVotes,
            AVG(d.averageRating) * SUM(d.numVotes) AS totalProduct,
            SUM(CASE WHEN d.hasAwards > 0 THEN 1 ELSE 0 END) AS moviesWithAwards,
            SUM(CASE WHEN d.hasAwards = 0 THEN 1 ELSE 0 END) AS moviesWithoutAwards
        FROM
            director_movies d
        GROUP BY
            d.directorId
    )
    SELECT
        da.directorId,
        n.primaryName AS directorName,
        da.movies AS movie,
        da.movie_count AS movieCount,
        a.awards,
        a.awardsCount,
        da.avgRating,
        da.totalVotes,
        da.totalProduct,
        da.moviesWithAwards,
        da.moviesWithoutAwards,
        ((3 * da.moviesWithAwards + 1 * da.moviesWithoutAwards)/da.movie_count) * da.avgRating *da.totalVotes AS finalAssessmentValue,
        (da.moviesWithAwards * 100 / da.movie_count) AS awardPercentage
    FROM
        director_aggregates da
    JOIN
        name_basics n ON da.directorId = n.nconst
    LEFT JOIN
        awards_concat a ON a.const = da.directorId
    """
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def producers_comparison(producerId1: str, producerId2: str):
    conn = get_connection()

    query = f"""
    WITH ranked_regions AS (
    SELECT
        a.titleId,
        b.primaryTitle,
        a.region,
        a.ordering,
        ROW_NUMBER() OVER (PARTITION BY a.titleId ORDER BY a.ordering DESC) AS rank
    FROM
        akas a
    JOIN
        basics b ON a.titleId = b.tconst
    WHERE
        LOWER(a.title) = LOWER(b.primaryTitle)
        AND a.isOriginalTitle = 0
        AND a.region != '\\N'
        AND LENGTH(a.region) < 3
    ),
    original_regions AS (
        SELECT
            titleId,
            primaryTitle,
            region
        FROM
            ranked_regions
        WHERE
            rank = 1
    ),
    awards_concat AS (
        SELECT
            const,
            GROUP_CONCAT(awardName || ' ' || categoryName || ' ' || year, ', ') AS awards,
            COUNT(*) AS awardsCount
        FROM
            awards
        GROUP BY
            const
    ),
    producer_movies AS (
        SELECT
            p.nconst AS producerId,
            p.tconst AS movieId,
            b.primaryTitle,
            r.averageRating,
            r.numVotes,
            r.averageRating * r.numVotes AS product
        FROM
            principals p
        JOIN
            basics b ON p.tconst = b.tconst
        JOIN
            ratings r ON p.tconst = r.tconst
        JOIN
            original_regions o ON p.tconst = o.titleId
        WHERE
            p.category = 'producer'
            AND p.nconst IN ('{producerId1}', '{producerId2}')
    ),
    producer_aggregates AS (
        SELECT
            p.producerId,
            COUNT(p.movieId) AS movie_count,
            GROUP_CONCAT(p.primaryTitle) AS movies,
            AVG(p.averageRating) AS avgRating,
            SUM(p.numVotes) AS totalVotes,
            AVG(p.averageRating) * SUM(p.numVotes) AS totalProduct
        FROM
            producer_movies p
        GROUP BY
            p.producerId
    )
    SELECT
        pa.producerId,
        n.primaryName AS producerName,
        pa.movies AS movie,
        pa.movie_count AS movieCount,
        a.awards,
        a.awardsCount,
        pa.avgRating,
        pa.totalVotes,
        pa.totalProduct
    FROM
        producer_aggregates pa
    JOIN
        name_basics n ON pa.producerId = n.nconst
    LEFT JOIN
        awards_concat a ON a.const = pa.producerId
    ORDER BY
        pa.totalProduct DESC;
    """
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def movies_comparison(movieId1: str, movieId2: str):
    conn = get_connection()

    query = f"""
    WITH ranked_regions AS (
    SELECT
        a.titleId,
        b.titleType,
        b.primaryTitle,
        a.region,
        a.ordering,
        ROW_NUMBER() OVER (PARTITION BY a.titleId ORDER BY a.ordering DESC) AS rank
    FROM
        akas a
    JOIN
        basics b ON a.titleId = b.tconst
    WHERE
        LOWER(a.title) = LOWER(b.primaryTitle)
        AND a.isOriginalTitle = 0
        AND a.region != '\\N'
        AND length(a.region) < 3
    ),
    original_regions AS (
        SELECT
            titleId,
            titleType,
            primaryTitle,
            region
        FROM
            ranked_regions
        WHERE
            rank = 1
            AND titleId IN ('{movieId1}', '{movieId2}')
    ),
    awards_concat AS (
        SELECT
            const,
            GROUP_CONCAT(awardName || ' ' || categoryName || ' ' || year, ', ') AS awards,
            COUNT(*) AS awards_count
        FROM
            awards
        GROUP BY
            const
    )
    SELECT
        o.titleId,
        o.titleType,
        o.primaryTitle,
        o.region,
        r.numVotes,
        r.averageRating,
        r.averageRating * r.numVotes AS product,
        a.awards,
        a.awards_count
    FROM
        original_regions o
    JOIN
        ratings r ON o.titleId = r.tconst
    LEFT JOIN
        awards_concat a ON o.titleId = a.const
    ORDER BY product DESC
    """
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def countries_comparison(country1: str, country2: str, genre=None):
    conn = get_connection()

    query = f"""
    WITH ranked_regions AS (
    SELECT
        a.titleId,
        b.primaryTitle,
        a.region,
        a.ordering,
        ROW_NUMBER() OVER (PARTITION BY a.titleId ORDER BY a.ordering DESC) AS rank
    FROM
        akas a
    JOIN
        basics b ON a.titleId = b.tconst
    WHERE
        LOWER(a.title) = LOWER(b.primaryTitle)
        {" AND b.genres = " "'" + genre + "'" if genre else ""}
        AND a.isOriginalTitle = 0
        AND a.region != '\\N'
        AND LENGTH(a.region) < 3
    ),
    original_regions AS (
        SELECT
            titleId,
            primaryTitle,
            region
        FROM
            ranked_regions
        WHERE
            rank = 1
    ),
    country_cinema_data AS (
        SELECT
            o.region,
            countries.country AS countryName,
            countries.population,
            countries.gdp_capita,
            countries.gdp_capita * countries.population AS gdp,
            COUNT(o.titleId) AS numFilms,
            SUM(r.numVotes) AS votes,
            AVG(r.averageRating) AS avgRating,
            SUM(r.numVotes) * AVG(r.averageRating) AS qualityScore,
            CAST(countries.population / COUNT(o.titleId) AS FLOAT) AS populationPerFilm,
            CAST(COUNT(o.titleId) / (countries.gdp_capita * countries.population) AS FLOAT) AS filmPerGdp,
            CAST((countries.gdp_capita * countries.population) / COUNT(o.titleId) AS FLOAT) AS gdpPerFilm,
            CAST(COUNT(o.titleId) / countries.gdp_capita AS FLOAT) AS gdpCapitaPerFilm
        FROM
            original_regions o
        JOIN ratings r ON o.titleId = r.tconst
        JOIN countries ON countries.abbreviation = o.region
        GROUP BY
            o.region
    ),
    country_ranks AS (
        SELECT
            countryName,
            gdp,
            numFilms,
            qualityScore,
            ROW_NUMBER() OVER (ORDER BY gdp DESC) AS gdp_rank,
            ROW_NUMBER() OVER (ORDER BY numFilms DESC) AS weak_cinematic_impact_rank,
            ROW_NUMBER() OVER (ORDER BY qualityScore DESC) AS strong_cinematic_impact_rank
        FROM
            country_cinema_data
    )
    SELECT
        ccd.region,
        ccd.countryName,
        ccd.population,
        ccd.gdp_capita,
        ccd.gdp,
        ccd.numFilms,
        ccd.votes,
        ccd.avgRating,
        ccd.qualityScore,
        ccd.populationPerFilm,
        ccd.filmPerGdp,
        ccd.gdpPerFilm,
        ccd.gdpCapitaPerFilm,
        cr.gdp_rank,
        cr.weak_cinematic_impact_rank,
        cr.strong_cinematic_impact_rank,
        (cr.gdp_rank - cr.weak_cinematic_impact_rank) AS weak_cinematic_impact_difference,
        (cr.gdp_rank - cr.strong_cinematic_impact_rank) AS strong_cinematic_impact_difference
    FROM
        country_cinema_data ccd
    JOIN
        country_ranks cr ON ccd.countryName = cr.countryName
    WHERE ccd.region IN ('{country1}', '{country2}')

    """
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df
