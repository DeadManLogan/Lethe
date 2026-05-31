from duckdb import DuckDBPyConnection
import logging

logger = logging.getLogger(__name__)

def get_row_count(con: DuckDBPyConnection, schema: str, table: str) -> int:
    """Get number of rows in a table"""
    result = con.execute(f"""
        SELECT COUNT(*) FROM {schema}.{table}
    """).fetchone()[0]
    
    return result

def table_exists(con: DuckDBPyConnection, schema: str, table: str) -> bool:
    """Check if table exists"""
    result = con.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
        AND table_name = '{table}'
    """).fetchone()[0]
    
    return result > 0

def ensure_schema(con: DuckDBPyConnection, schema: str):
    """Create target schema if it doesn't exist"""
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    logger.debug(f"Ensured schema exists: {schema}")

def get_columns(
    con: DuckDBPyConnection,
    schema: str,
    table: str
) -> list[tuple]:
    """Get column names and types of a table"""
    result = con.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{table}'
        AND table_name = '{schema}'
        ORDER BY ordinal_position
    """).fetchall()

    return result

def drop_table(
    con: DuckDBPyConnection,
    schema: str,
    table: str
):
    """Drop a table"""
    con.execute(f"DROP TABLE IF EXISTS {schema}.{table}")