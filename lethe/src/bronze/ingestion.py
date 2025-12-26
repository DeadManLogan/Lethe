from lethe.config import Settings
from lethe.src.readers.csv_reader import CSVReader

settings = Settings()
# csv_reader = CSVReader(
#     'lethe/data/raw/financial_fraud_detection_dataset.csv',
#     'bronze',
#     'transaction_raw'
# )

csv_reader = CSVReader("lethe/data/raw/test.csv", "bronze", "transaction_raw")

csv_reader.validate_path()

# con = connect(settings.DATA_DIR)

# csv_reader.ingest(con)

# print(
#     con.execute(
#         """
# DESCRIBE bronze.transaction_raw
# """,
#     ).fetchall(),
# )


# con.close()
