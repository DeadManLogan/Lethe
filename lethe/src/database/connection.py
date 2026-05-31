from pathlib import Path
from lethe.config import Settings
import duckdb

settings = Settings()


def connect() -> duckdb.DuckDBPyConnection:
    db_path = Path(settings.DATA_DIR) / "lethe.duckdb"
    return duckdb.connect(db_path)

def disconnect(con: duckdb.DuckDBPyConnection):
    con.close()
