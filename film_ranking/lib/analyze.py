import sqlite3
import pandas as pd
from typing import Literal, Optional

CinematicRankSort = Literal["impact_score", "awards_count"]


def get_movies_with_regional_data(yearStart: int, yearEnd: int):
    conn = sqlite3.connect("./processed_data/film.db")

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
    ORDER BY ccd.qualityScore DESC;
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
    conn = sqlite3.connect("./processed_data/film.db")

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


def get_directors_rank(type: str, yearStart: int, yearEnd: int):
    conn = sqlite3.connect("./processed_data/film.db")

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
        AND b.titleType = '{type}'
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
            AND b.titleType = '{type}'
    ),
    director_aggregates AS (
        SELECT
            d.directorId,
            COUNT(d.movieId) AS {type}_count,
            GROUP_CONCAT(d.primaryTitle) AS {type}s,
            AVG(d.averageRating) AS avgRating,
            SUM(d.numVotes) AS totalVotes,
            AVG(d.averageRating) * SUM(d.numVotes) AS totalProduct,
            SUM(CASE WHEN d.hasAwards > 0 THEN 1 ELSE 0 END) AS {type}sWithAwards,
            SUM(CASE WHEN d.hasAwards = 0 THEN 1 ELSE 0 END) AS {type}sWithoutAwards
        FROM
            director_movies d
        GROUP BY
            d.directorId
    )
    SELECT
        da.directorId,
        n.primaryName AS directorName,
        da.{type}s AS {type},
        da.{type}_count AS {type}Count,
        a.awards,
        a.awardsCount,
        da.avgRating,
        da.totalVotes,
        da.totalProduct,
        da.{type}sWithAwards,
        da.{type}sWithoutAwards,
        ((3 * da.{type}sWithAwards + 1 * da.{type}sWithoutAwards)/da.{type}_count) * da.avgRating *da.totalVotes AS finalAssessmentValue,
        (da.{type}sWithAwards * 100 / da.{type}_count) AS awardPercentage
    FROM
        director_aggregates da
    JOIN
        name_basics n ON da.directorId = n.nconst
    LEFT JOIN
        awards_concat a ON a.const = da.directorId
    ORDER BY
        finalAssessmentValue DESC;
"""
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def get_producers_rank(type: str, yearStart, yearEnd):
    conn = sqlite3.connect("./processed_data/film.db")

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
        AND b.titleType = '{type}'
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
            AND b.titleType = '{type}'
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
        pa.movies AS {type},
        pa.movie_count AS {type}Count,
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


def get_actors_rank(yearStart: int, yearEnd: int):
    conn = sqlite3.connect("./processed_data/film.db")

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
    ),
    actor_ranking AS (
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
        ORDER BY
            aa.totalProduct DESC
    )
    SELECT
        ar.*
    FROM
        actor_ranking ar
"""
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def actors_comparison(actorId1: str, actorId2: str):
    conn = sqlite3.connect("../processed_data/film.db")

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
        WHERE
            actorId IN ('{actorId2}', '{actorId2}')
        ORDER BY
            aa.totalProduct DESC;
    """
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def directors_comparison(directorId1: str, directorId2: str):
    conn = sqlite3.connect("../processed_data/film.db")

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
        AND b.titleType = '{type}'
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
            AND b.titleType = '{type}'
    ),
    director_aggregates AS (
        SELECT
            d.directorId,
            COUNT(d.movieId) AS {type}_count,
            GROUP_CONCAT(d.primaryTitle) AS {type}s,
            AVG(d.averageRating) AS avgRating,
            SUM(d.numVotes) AS totalVotes,
            AVG(d.averageRating) * SUM(d.numVotes) AS totalProduct,
            SUM(CASE WHEN d.hasAwards > 0 THEN 1 ELSE 0 END) AS {type}sWithAwards,
            SUM(CASE WHEN d.hasAwards = 0 THEN 1 ELSE 0 END) AS {type}sWithoutAwards
        FROM
            director_movies d
        GROUP BY
            d.directorId
    )
    SELECT
        da.directorId,
        n.primaryName AS directorName,
        da.{type}s AS {type},
        da.{type}_count AS {type}Count,
        a.awards,
        a.awardsCount,
        da.avgRating,
        da.totalVotes,
        da.totalProduct,
        da.{type}sWithAwards,
        da.{type}sWithoutAwards,
        ((3 * da.{type}sWithAwards + 1 * da.{type}sWithoutAwards)/da.{type}_count) * da.avgRating *da.totalVotes AS finalAssessmentValue,
        (da.{type}sWithAwards * 100 / da.{type}_count) AS awardPercentage
    FROM
        director_aggregates da
    JOIN
        name_basics n ON da.directorId = n.nconst
    LEFT JOIN
        awards_concat a ON a.const = da.directorId
    WHERE da.directorId IN ('{directorId1}', '{directorId2}')
    ORDER BY
        finalAssessmentValue DESC;
    """
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def producers_comparison(producerId1: str, producerId2: str):
    conn = sqlite3.connect("../processed_data/film.db")

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
    WHERE pa.producerId IN ('{producerId1}', '{producerId2}')
    ORDER BY
        pa.totalProduct DESC;
    """
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def cinematics_comparison(cinematicId1: str, cinematicId2: str):
    conn = sqlite3.connect("../processed_data/film.db")

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
    WHERE o.titleId IN ('{cinematicId1}', '{cinematicId2}')
    ORDER BY product DESC
    """
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df


def countries_comparison(country1: str, country2: str):
    conn = sqlite3.connect("./processed_data/film.db")

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
    ORDER BY ccd.qualityScore DESC;
    """
    df = pd.read_sql_query(query, conn)

    df.head()

    conn.close()
    return df
