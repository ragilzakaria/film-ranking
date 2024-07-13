from lib.load_data import (
    load_movies_akas,
    load_movies_basics,
    load_countries_data,
    load_movies_crew,
    load_movie_principals,
    load_movie_ratings,
    load_name_basics,
    load_episodes,
    load_events_data,
    update_country_of_origin,
)

if __name__ == "__main__":
    # load_movies_akas("./data/title.akas.tsv")
    # load_movies_basics("./data/title.basics.tsv")
    # load_countries_data("./data/countries.tsv")
    # load_movies_crew("./data/title.crew.tsv")
    # load_movie_principals("./data/title.principals.tsv")
    # load_movie_ratings("./data/title.ratings.tsv")
    # load_name_basics("./data/name.basics.tsv")
    # load_episodes("./data/title.episode.tsv")
    # load_events_data("./data/awards.csv")
    update_country_of_origin()
    print("Hello world!")
