from duckdb import DuckDBPyConnection

class CSVReader:
    def __init__(self, path: str, schema: str, table: str):
        self.path = path
        self.schema = schema
        self.table = table

    def ingest(self, con: DuckDBPyConnection):
        con.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {self.schema};

            DROP TABLE IF EXISTS {self.schema}.{self.table};

            CREATE TABLE {self.schema}.{self.table} AS
            SELECT *
            FROM read_csv_auto('{self.path}', header=true);
        """)
