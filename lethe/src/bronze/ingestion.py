from lethe.src.readers.csv_reader import CSVReader
from duckdb import DuckDBPyConnection



def start_ingestion(con: DuckDBPyConnection):
    """Ingest all raw files to DuckDB.

    Create the necessary readers that will ingest
    files as tables in DuckDB.
    """
    csv_reader = CSVReader(
        "lethe/data/raw/financial_fraud_detection_dataset.csv", "bronze", "transactions_raw"
    )

    csv_reader.ingest(con)
    csv_reader.table_preview(con)
    csv_reader.table_details(con)
