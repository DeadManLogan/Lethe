import re
from pathlib import Path

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
def test_validate_identifiers(self, schema: str, table: str):
    """Test valid identifirers pass."""
    reader = CSVReader("dummy.csv", schema, table)
    reader.validate_identifiers()


@pytest.mark.parametrize("schema, table, expected_invalid", INVALID_IDENTIFIERS)
def test_validate_identifiers_invalid(self, schema: str, table: str, expected_invalid: str):
    """Test that invalid identifiers are rejected."""
    reader = CSVReader("dummy.csv", schema, table)

    with pytest.raises(ValueError, match=f"Invalid identifier: {re.escape(expected_invalid)}"):
        reader.validate_identifiers()


@pytest.mark.parametrize("schema, table", SQL_INJECTION_ATTEMPTS)
def test_validate_identifiers_sql_injection(self, schema: str, table: str):
    """Test that SQL injection attempts are blocked."""
    reader = CSVReader("dummy.csv", schema, table)

    with pytest.raises(ValueError):
        reader.validate_identifiers()
