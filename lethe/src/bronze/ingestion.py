from lethe.config import Settings
from lethe.src.database.connection import connect
from lethe.src.readers.csv_reader import CSVReader

settings = Settings()


def start_ingestion():
    """Ingest all raw files to DuckDB.

    Create the necessary readers that will ingest
    files as tables in DuckDB.
    """
    csv_reader = CSVReader(
        "lethe/data/raw/financial_fraud_detection_dataset.csv", "bronze", "transaction_raw"
    )

    con = connect(settings.DATA_DIR)

    csv_reader.drop_table(con)

    csv_reader.ingest(con)
    csv_reader.table_preview(con)
    csv_reader.table_details(con)

    con.close()
