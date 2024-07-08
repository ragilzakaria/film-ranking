import pytest
from film_ranking.lib.analyze import gdp_per_capita_to_normal_gdp


def test_gdp_per_capita_to_normal_gdp():
    assert gdp_per_capita_to_normal_gdp(1000, 1000_000) == 1000_000_000
