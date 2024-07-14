import argparse
import os

from colorama import init, Fore
import papermill as pm
from datetime import datetime
import sys
import nbformat
from nbconvert import HTMLExporter
import io

from lib.util import print_color

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

from pathlib import Path


def get_project_root():
    """
    Find the root directory of the project.
    This looks for the first directory in the path that doesn't contain an __init__.py file.
    """
    path = os.path.abspath(".")
    while "__init__.py" in os.listdir(path):
        parent = os.path.dirname(path)
        if parent == path:  # We've reached the root directory
            break
        path = parent
    return path


def get_absolute_path(relative_path):
    """
    Get the absolute path of a file relative to the project root.
    """
    project_root = get_project_root()
    return os.path.normpath(os.path.join(project_root, relative_path))


def display_notebook_output(notebook_path):
    with open(notebook_path, "r", encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)

    for cell in nb.cells:
        if cell.cell_type == "code":
            if cell.outputs:
                for output in cell.outputs:
                    if "text" in output:
                        print(output.text)
                    elif "data" in output:
                        if "text/plain" in output.data:
                            print(output.data["text/plain"])
            print()  # Add a blank line between cell outputs


def load_data(folder, load_data_service):
    print_color(f"Loading data from folder: {folder}", Fore.CYAN)
    load_data_service(folder)


def analyze_top(global_args, category, **kwargs):
    # type=args.type, genre=args.genre, country=args.country
    print_color(f"Analyzing top {category}", Fore.GREEN)

    if category == "countries":
        notebook = "./notebook/top_countries.ipynb"
        output_notebook = f"./processed_data/{notebook}"
        os.makedirs(os.path.dirname(notebook), exist_ok=True)
        os.makedirs(os.path.dirname(output_notebook), exist_ok=True)

        print_color(f"Executing notebook {notebook}", Fore.WHITE)
        pm.execute_notebook(
            notebook,
            output_notebook,
            parameters=dict(
                start_year=global_args.start_year, end_year=global_args.end_year
            ),
        )
        display_notebook_output(output_notebook)
        print_color(
            f"To learn more open the output at {get_absolute_path(output_notebook)}",
            Fore.GREEN,
        )
    else:
        print_color("Not implemented...", Fore.RED)

    # Implement analysis logic here
    for key, value in kwargs.items():
        print(f"  {key}: {value}")


def compare(category, item1, item2):
    print_color(f"Comparing {category}: {item1} vs {item2}", Fore.YELLOW)
    # Implement comparison logic here


def run_cli(load_data_service):
    parser = argparse.ArgumentParser(description="Film Ranking CLI")
    subparsers = parser.add_subparsers(dest="command", help="Sub-command help")

    # Global options
    current_year = datetime.now().year
    parser.add_argument(
        "-start-year",
        type=int,
        help="The start year for the analysis (default: analyze all)",
    )
    parser.add_argument(
        "-end-year",
        type=int,
        help="The end year for the analysis (default: current year)",
    )

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

    # Validate global arguments
    if args.start_year and args.end_year and args.start_year > args.end_year:
        print_color(
            "Error: Start year must be less than or equal to end year.", Fore.RED
        )
        sys.exit(1)

    if args.end_year and args.end_year > current_year:
        print_color(
            f"Warning: End year is in the future. Using current year ({current_year}) instead.",
            Fore.YELLOW,
        )
        args.end_year = current_year

    if args.command == "load_data":
        load_data(args.folder, load_data_service)
    elif args.command == "analyze":
        if args.analyze_type == "top_movies":
            analyze_top(
                args, "movies", type=args.type, genre=args.genre, country=args.country
            )
        elif args.analyze_type:
            analyze_top(args, args.analyze_type.split("_")[1])
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
