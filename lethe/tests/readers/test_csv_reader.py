import pytest

from lethe.src.readers.csv_reader import CSVReader


class TestCSVReader:
    """Unit tests for CSVReader class."""

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

    @pytest.mark.parametrize("schema, table", VALID_IDENTIFIERS)
    def test_validate_identifiers(self, schema: str, table: str):
        """Test valid identifirers pass."""
        reader = CSVReader("dummy.csv", schema, table)
        reader.validate_identifiers()
