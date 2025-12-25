from pathlib import Path

import duckdb


def connect(data_dir: str):
    db_path = Path(data_dir) / "lethe.duckdb"
    return duckdb.connect(db_path)
