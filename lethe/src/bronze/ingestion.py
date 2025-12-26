from lethe.config import Settings
from lethe.src.database.connection import connect
from lethe.src.readers.csv_reader import CSVReader

settings = Settings()
csv_reader = CSVReader(
    "lethe/data/raw/financial_fraud_detection_dataset.csv", "bronze", "transaction_raw"
)

con = connect(settings.DATA_DIR)

csv_reader.ingest(con)

con.close()
