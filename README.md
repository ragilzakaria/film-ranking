# Film Ranking Tool

## How to run on your machine

### Install dependencies

```bash
poetry install
```

### Install Linter in pre-commit

```bash
pre-commit install
pre-commit run --all-files
```

## How to use

## Load data

The first step of using the CLI is to load the data, the data can be in any folder but it should look like this

```
data
├── awards.csv
├── countries.tsv
├── name.basics.tsv
├── title.akas.tsv
├── title.basics.tsv
├── title.crew.tsv
├── title.episode.tsv
├── title.principals.tsv
└── title.ratings.tsv

```

and then run it using

```bash
python film_ranking load_data data
```

Please note that I have two additional data which are countries.tsv and awards.csv.
The data is from:
- https://www.kaggle.com/datasets/fernandol/countries-of-the-world
- https://www.kaggle.com/datasets/iwooloowi/film-awards-imdb

## to get countries ranking, sort_by is optional
```bash
python film_ranking -start-year 1900 -end-year 2024 analyze top_countries -sort_by gdp
```

## to get movies ranking. limit, genre, mtype, and country are optional
```bash
python film_ranking -start-year 1900 -end-year 2024 analyze top_movies -limit 100 -genre Family -type movie -country US -sort_by awards_count
```

## to get directors ranking, sort_by is optional
```bash
python film_ranking -start-year 1900 -end-year 2024 analyze top_directors -sort_by awardsCount
```

## to get producers ranking, sort_by is optional
```bash
python film_ranking -start-year 1900 -end-year 2024 analyze top_producers -sort_by movieCount
```

## to get actors ranking, sort_by is optional
```bash
python film_ranking -start-year 1900 -end-year 2024 analyze top_actors -sort_by countryCount
```

## to compare actors
```bash
python film_ranking compare actor nm0000001 nm0000002
```

## to compare directors
```bash
python film_ranking compare director nm0000001 nm0000002
```

## to compare producers
```bash
python film_ranking compare producer nm0000001 nm0000002
```

## to compare movies
```bash
python film_ranking compare movie tt0000001 tt0000002
```

## to compare countries, genre is optional
```bash
python film_ranking compare country ID NL -genre Family
```

## search film
```bash
python film_ranking search movie -keyword 'Captain America' -limit 20
```

## search person
```bash
python film_ranking search person -keyword 'Chris' -limit 20
```

# Profiling

## Example profiling

Run the profiling

```bash
python -m cProfile -o output.prof film_ranking/__main__.py analyze top_countries -sort_by gdp
```

Analyze the profile

```bash
snakeviz output.prof
```


# How to build

```bash
poetry build
```

Then after build you'll get a `dist` folder with the `whl` and `tar.gz` file. You can upload it as release.

## How to install

```bash
pip install https://github.com/ragilzakaria/film-ranking/releases/download/v0.1.0/film_ranking-0.1.0-py3-none-any.whl
```
