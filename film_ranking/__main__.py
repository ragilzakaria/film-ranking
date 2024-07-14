import os

from colorama import Fore

from .lib.util import print_color
from .lib.load_data import (
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
from .cli import run_cli


def load_data_service(folder: str):
    os.makedirs(folder, exist_ok=True)
    print_color("Loading movies...", Fore.WHITE)
    load_movies_akas(f"./{folder}/title.akas.tsv")

    print_color("Loading basics...", Fore.WHITE)
    load_movies_basics(f"./{folder}/title.basics.tsv")

    print_color("Loading countries...", Fore.WHITE)
    load_countries_data(f"./{folder}/countries.tsv")

    print_color("Loading crew...", Fore.WHITE)
    load_movies_crew(f"./{folder}/title.crew.tsv")

    print_color("Loading principals...", Fore.WHITE)
    load_movie_principals(f"./{folder}/title.principals.tsv")

    print_color("Loading ratings...", Fore.WHITE)
    load_movie_ratings(f"./{folder}/title.ratings.tsv")

    print_color("Loading name basics...", Fore.WHITE)
    load_name_basics(f"./{folder}/name.basics.tsv")

    print_color("Loading episode...", Fore.WHITE)
    load_episodes(f"./{folder}/title.episode.tsv")

    print_color("Loading awards...", Fore.WHITE)
    load_events_data(f"./{folder}/awards.csv")


def main():
    run_cli(load_data_service)


if __name__ == "__main__":
    main()
