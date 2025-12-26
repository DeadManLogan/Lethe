from pathlib import Path

from duckdb import DuckDBPyConnection


class CSVReader:
    """Ingest csv file to DuckDB.
    
    Accepts local file path, db schema and table as args.
    Validates them. Creates if they don't exist.
    """

    def __init__(self, path: str, schema: str, table: str):
        """Construct instance parameters necessary for table creation."""
        self.path = path
        self.schema = schema
        self.table = table

    def validate_path(self):
        """Check that csv file path exists."""
        path = Path(self.path)
        if not path.exists():
            raise FileNotFoundError(f"Path {self.path} doesn't exist.")

    def ingest(self, con: DuckDBPyConnection):
        con.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {self.schema};

            DROP TABLE IF EXISTS {self.schema}.{self.table};

            CREATE TABLE {self.schema}.{self.table} AS
            SELECT *
            FROM read_csv_auto('{self.path}', header=true);
        """
        )

        # todo
        # logger for reader
        # break the ingeset to several private methods (schema, drop, create)
