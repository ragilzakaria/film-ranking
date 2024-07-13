import pytest
import pandas as pd
from io import StringIO
from film_ranking.lib.load_data import lazy_pandas_csv_reader


@pytest.fixture
def csv_file(tmp_path):
    content = """id,name,age
1,Alice,30
2,Bob,25
3,Charlie,35
4,David,28
5,Eve,32
"""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(content)
    return str(csv_file)


def test_row_count(csv_file):
    rows = list(lazy_pandas_csv_reader(csv_file, chunksize=2, delimiter=","))
    assert len(rows) == 5  # 5 data rows, excluding header


def test_data_correctness(csv_file):
    rows = list(lazy_pandas_csv_reader(csv_file, chunksize=2, delimiter=","))
    expected_data = [
        (1, "Alice", 30),
        (2, "Bob", 25),
        (3, "Charlie", 35),
        (4, "David", 28),
        (5, "Eve", 32),
    ]
    assert rows == expected_data


@pytest.mark.parametrize("chunksize", [1, 2, 5, 10])
def test_different_chunk_sizes(csv_file, chunksize):
    rows = list(lazy_pandas_csv_reader(csv_file, chunksize=chunksize, delimiter=","))
    assert len(rows) == 5


def test_different_delimiter(tmp_path):
    content = """id;name;age
1;Alice;30
2;Bob;25
"""
    csv_file = tmp_path / "test_semicolon.csv"
    csv_file.write_text(content)

    rows = list(lazy_pandas_csv_reader(str(csv_file), chunksize=2, delimiter=";"))
    assert len(rows) == 2
    assert rows[0] == (1, "Alice", 30)
    assert rows[1] == (2, "Bob", 25)
