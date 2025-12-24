import duckdb
from pathlib import Path

def connect(data_dir: str):
    DB_PATH = Path(data_dir) / "lethe.duckdb"
    return duckdb.connect(DB_PATH)
