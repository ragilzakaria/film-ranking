import argparse
import sys
from datetime import datetime
from colorama import init, Fore, Style

# Initialize colorama
init(autoreset=True)

# Constants
MOVIE_GENRES = [
    "Documentary",
    "Animation",
    "Comedy",
    "Short",
    "Romance",
    "News",
    "Drama",
    "Fantasy",
    "Horror",
    "Biography",
    "Music",
    "Crime",
    "Family",
    "Adventure",
    "Action",
    "History",
    "Mystery",
    "Musical",
    "War",
    "Sci-Fi",
    "Western",
    "Thriller",
    "Sport",
    "Film-Noir",
    "Talk-Show",
    "Game-Show",
    "Adult",
    "Reality-TV",
]

MOVIE_TYPES = [
    "short",
    "movie",
    "tvEpisode",
    "tvMiniSeries",
    "tvMovie",
    "tvPilot",
    "tvSeries",
    "tvShort",
    "tvSpecial",
    "video",
    "videoGame",
]


def print_color(text, color):
    print(f"{color}{text}{Style.RESET_ALL}")


def load_data(folder):
    print_color(f"Loading data from folder: {folder}", Fore.CYAN)
    # Implement data loading logic here


def analyze_top(category, **kwargs):
    print_color(f"Analyzing top {category}", Fore.GREEN)
    # Implement analysis logic here
    for key, value in kwargs.items():
        print(f"  {key}: {value}")


def compare(category, item1, item2):
    print_color(f"Comparing {category}: {item1} vs {item2}", Fore.YELLOW)
    # Implement comparison logic here


def run_cli():
    parser = argparse.ArgumentParser(description="Film Ranking CLI")
    subparsers = parser.add_subparsers(dest="command", help="Sub-command help")

    # Load data command
    load_parser = subparsers.add_parser("load_data", help="Load data from a folder")
    load_parser.add_argument("folder", help="Folder containing data files")

    # Analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze data")
    analyze_subparsers = analyze_parser.add_subparsers(
        dest="analyze_type", help="Analysis type"
    )

    # Top director, actor, producer
    for category in ["director", "actor", "producer"]:
        analyze_subparsers.add_parser(f"top_{category}", help=f"Analyze top {category}")

    # Top movies
    top_movies_parser = analyze_subparsers.add_parser(
        "top_movies", help="Analyze top movies"
    )
    top_movies_parser.add_argument("-type", choices=MOVIE_TYPES, help="Movie type")
    top_movies_parser.add_argument("-genre", choices=MOVIE_GENRES, help="Movie genre")
    top_movies_parser.add_argument("-country", help="Two-digit country code")

    # Top countries
    analyze_subparsers.add_parser("top_countries", help="Analyze top countries")

    # Compare command
    compare_parser = subparsers.add_parser("compare", help="Compare items")
    compare_subparsers = compare_parser.add_subparsers(
        dest="compare_type", help="Comparison type"
    )

    for category in ["director", "actor", "producer", "movie", "country"]:
        cat_parser = compare_subparsers.add_parser(
            category, help=f"Compare {category}s"
        )
        cat_parser.add_argument(f"{category}1", help=f"First {category} to compare")
        cat_parser.add_argument(f"{category}2", help=f"Second {category} to compare")

    args = parser.parse_args()

    if args.command == "load_data":
        load_data(args.folder)
    elif args.command == "analyze":
        if args.analyze_type == "top_movies":
            analyze_top(
                "movies", type=args.type, genre=args.genre, country=args.country
            )
        elif args.analyze_type:
            analyze_top(args.analyze_type.split("_")[1])
        else:
            print_color("Error: Please specify an analysis type", Fore.RED)
    elif args.command == "compare":
        if args.compare_type:
            compare(
                args.compare_type,
                getattr(args, f"{args.compare_type}1"),
                getattr(args, f"{args.compare_type}2"),
            )
        else:
            print_color("Error: Please specify a comparison type", Fore.RED)
    else:
        parser.print_help()
