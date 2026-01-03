import logging
import re
from pathlib import Path

from duckdb import DuckDBPyConnection

logger = logging.getLogger(__name__)


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
        logger.info(
            "Starting CSV ingestion: path=%s schema=%s table=%s", self.path, self.schema, self.table
        )

        self.validate()
        self.create_schema(con)

        if self.table_exists(con):
            logger.info("Table %s.%s already exists, skipping ingestion", self.schema, self.table)
            return

        self.ingest_raw(con)
        logger.info("Successfully ingested table %s.%s", self.schema, self.table)

    def create_schema(self, con: DuckDBPyConnection):
        """Create DuckDB schema."""
        logger.debug("Ensuring schema exists: %s", self.schema)

        con.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {self.schema};
        """
        )

    def ingest_raw(self, con: DuckDBPyConnection):
        """Create DuckDB table based on csv file."""
        logger.info("Ingesting raw CSV data into %s.%s", self.schema, self.table)

        con.execute(
            f"""
            CREATE TABLE {self.schema}.{self.table} AS
            SELECT *
            FROM read_csv_auto('{self.path}', header=true);
        """
        )

    def drop_table(self, con: DuckDBPyConnection):
        """Drop DuckDB table."""
        logger.warning("Dropping table %s.%s if it exists", self.schema, self.table)

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
        logger.debug("Checking if table exists: %s.%s", self.schema, self.table)

        result = con.execute(
            f"""
            SELECT COUNT(*) 
            FROM information_schema.tables
            WHERE table_schema = '{self.schema}'
            AND table_name = '{self.table}';
        """
        ).fetchone()[0]

        logger.debug("Table existence check for %s.%s: %s", self.schema, self.table, result > 0)
        return result > 0

    def validate(self):
        """Orchestrate validation."""
        logger.debug("Validating CSVReader inputs")

        self.validate_path()
        self.validate_identifiers()

    def validate_path(self):
        """Check that csv file path exists."""
        logger.debug("Validating CSV path: %s", self.path)

        path = Path(self.path)
        if not path.exists():
            logger.error("CSV path does not exist: %s", self.path)
            raise FileNotFoundError(f"Path {self.path} doesn't exist.")

    def validate_identifiers(self):
        """Use regex pattern to prevent SQL injection."""
        logger.debug("Validating identifiers: schema=%s table=%s", self.schema, self.table)

        pattern = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
        for value in [self.schema, self.table]:
            if not pattern.match(value):
                logger.error("Invalid SQL identifier detected: %s", value)
                raise ValueError(f"Invalid identifier: {value}")

    def table_preview(self, con: DuckDBPyConnection) -> list[tuple]:
        """Return 5 rows of table data."""
        logger.debug("Previewing first 5 rows of %s.%s", self.schema, self.table)
        result = con.execute(
            f"""
            SELECT * 
            FROM {self.schema}.{self.table}
            LIMIT 5;
        """
        ).fetchall()
        logger.info(result)

        return result

    def table_details(self, con: DuckDBPyConnection) -> list:
        """Describe table."""
        logger.debug("Describing table %s.%s", self.schema, self.table)
        details = con.execute(
            f"""
            DESCRIBE {self.schema}.{self.table};
            """
        ).fetchall()
        logger.info(details)

        return details
