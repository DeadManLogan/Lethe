import re
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

    def ingest(self, con: DuckDBPyConnection):
        """Create table once, skip if exists."""
        self.validate()
        self.create_schema(con)

        if self.table_exists(con):
            print(f"Table {self.table} already exists")
            return

        self.ingest_raw(con)

    def create_schema(self, con: DuckDBPyConnection):
        """Create DuckDB schema."""
        con.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {self.schema};
        """
        )

    def ingest_raw(self, con: DuckDBPyConnection):
        """Create DuckDB table based on csv file."""
        con.execute(
            f"""
            CREATE TABLE {self.schema}.{self.table} AS
            SELECT *
            FROM read_csv_auto('{self.path}', header=true);
        """
        )

    def drop_table(self, con: DuckDBPyConnection):
        """Drop DuckDB table."""
        con.execute(
            f"""
            DROP TABLE IF EXISTS {self.schema}.{self.table};
        """
        )

    def table_exists(self, con: DuckDBPyConnection) -> bool:
        """Check whether target table already exists.

        Query DuckDB metadata about the table
        and not actual data.The query returns 0 or 1,
        depends on the existance of the table.
        """
        result = con.execute(
            f"""
            SELECT COUNT(*) 
            FROM information_schema.tables
            WHERE table_schema = '{self.schema}'
            AND table_name = '{self.table}';
        """
        ).fetchone()[0]

        return result > 0

    def validate(self):
        """Orchestrate validation."""
        self.validate_path()
        self.validate_identifiers()

    def validate_path(self):
        """Check that csv file path exists."""
        path = Path(self.path)
        if not path.exists():
            raise FileNotFoundError(f"Path {self.path} doesn't exist.")

    def validate_identifiers(self):
        """Use regex pattern to prevent SQL injection."""
        pattern = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
        for value in [self.schema, self.table]:
            if not pattern.match(value):
                raise ValueError(f"Invalid identifier: {value}")

        # todo
        # logger for reader
        # break the ingeset to several private methods (schema, drop, create)
