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


## to get countries ranking, sort_by is optional
```bash
python film_ranking -start-year 0000 -end-year 0001 analyze top_countries -sort_by gdp
```

## to get movies ranking. limit, genre, mtype, and country are optional
```bash
python film_ranking -start-year 0000 -end-year 0001 analyze top_movies -limit 100 -genre Genre -mtype movie -country US -sort_by awards_count/impact_score
```

## to get directors ranking, sort_by is optional
```bash
python film_ranking -start year 0000 -end-year 0001 analyze top_directors -sort_by awardsCount
```

## to get producers ranking, sort_by is optional
```bash
python film_ranking -start year 0000 -end-year 0001 analyze top_producers -sort_by movieCount
```

## to get actors ranking, sort_by is optional
```bash
python film_ranking -start year 0000 -end-year 0001 analyze top_actors -sort_by countryCount
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

```
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
