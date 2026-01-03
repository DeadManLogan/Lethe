import re
from pathlib import Path

import duckdb
import pytest

from lethe.src.readers.csv_reader import CSVReader

VALID_IDENTIFIERS = [
    ("raw", "transactions"),
    ("RAW", "TRANSACTIONS"),
    ("RawData", "FraudTrans"),
    ("_private", "_temp"),
    ("schema123", "table_v2"),
    ("raw_data", "fraud_transactions"),
    ("raw__data", "fraud___trans"),
    ("a", "b"),
    ("_", "__"),
]

INVALID_IDENTIFIERS = [
    ("123schema", "transactions", "123schema"),
    ("raw", "2023_table", "2023_table"),
    ("0_schema", "transactions", "0_schema"),
    ("raw-data", "transactions", "raw-data"),
    ("raw data", "transactions", "raw data"),
    ("raw.data", "transactions", "raw.data"),
    ("raw;drop", "transactions", "raw;drop"),
    ("raw'data", "transactions", "raw'data"),
    ('raw"data', "transactions", 'raw"data'),
    ("raw$data", "transactions", "raw$data"),
    ("raw@data", "transactions", "raw@data"),
    ("raw#data", "transactions", "raw#data"),
    ("raw(data)", "transactions", "raw(data)"),
    ("", "transactions", ""),
    ("raw", "", ""),
]

SQL_INJECTION_ATTEMPTS = [
    ("raw; DROP TABLE users; --", "transactions"),
    ("raw", "transactions' OR '1'='1"),
    ("raw", "transactions' UNION SELECT * FROM pwd--"),
    ("raw--comment", "transactions"),
    ("raw/*comment*/", "transactions"),
    ("raw'; DELETE FROM users WHERE '1'='1", "transactions"),
]


@pytest.mark.parametrize("should_exist", [True, False])
def test_validate_path(tmp_path: Path, should_exist: bool):
    """Test whether the path exist or not."""
    path = tmp_path / "file.csv"

    if should_exist:
        path.write_text("a,b\n1,2")

    reader = CSVReader(
        path=str(path),
        schema="schema",
        table="table",
    )

    if should_exist:
        reader.validate_path()
    else:
        with pytest.raises(FileNotFoundError):
            reader.validate_path()


@pytest.mark.parametrize("schema, table", VALID_IDENTIFIERS)
def test_validate_identifiers(schema: str, table: str):
    """Test valid identifirers pass."""
    reader = CSVReader("dummy.csv", schema, table)
    reader.validate_identifiers()


@pytest.mark.parametrize("schema, table, expected_invalid", INVALID_IDENTIFIERS)
def test_validate_identifiers_invalid(schema: str, table: str, expected_invalid: str):
    """Test that invalid identifiers are rejected."""
    reader = CSVReader("dummy.csv", schema, table)

    with pytest.raises(ValueError, match=f"Invalid identifier: {re.escape(expected_invalid)}"):
        reader.validate_identifiers()


@pytest.mark.parametrize("schema, table", SQL_INJECTION_ATTEMPTS)
def test_validate_identifiers_sql_injection(schema: str, table: str):
    """Test that SQL injection attempts are blocked."""
    reader = CSVReader("dummy.csv", schema, table)

    with pytest.raises(ValueError):
        reader.validate_identifiers()


def test_table_exists_false():
    """Assert False because table doesn't exist in db."""
    con = duckdb.connect(":memory:")
    reader = CSVReader("dummy.csv", "test_schema", "test_table")

    con.execute("CREATE SCHEMA test_schema")
    assert reader.table_exists(con) is False


def test_table_exists_true():
    """Assert True for table in db."""
    con = duckdb.connect(":memory:")
    reader = CSVReader("dummy.csv", "test_schema", "test_table")

    con.execute("CREATE SCHEMA test_schema")
    con.execute("""CREATE TABLE test_schema.test_table (id INTEGER)""")
    assert reader.table_exists(con) is True

def test_ingest_creates_table_when_missing(tmp_path: Path):
    """Test that table is created when it's new."""
    csv = tmp_path / "data.csv"
    csv.write_text("id,name\n1,Alice\n2,Bob\n")

    con = duckdb.connect(":memory:")
    reader = CSVReader(str(csv), "test_schema", "people")

    reader.ingest(con)

    assert reader.table_exists(con) is True

def test_ingest_skips_when_table_exists(tmp_path: Path):
    """Test the skip of the table creation."""
    csv = tmp_path / "data.csv"
    csv.write_text("id\n1\n")

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA test_schema")
    con.execute("CREATE TABLE test_schema.people (id INTEGER)")

    reader = CSVReader(str(csv), "test_schema", "people")

    reader.ingest(con)

    result = con.execute(
        "SELECT COUNT(*) FROM test_schema.people"
    ).fetchone()[0]

    assert result == 0

