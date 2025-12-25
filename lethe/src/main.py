from lethe.config import Settings
from database.connection import connect

settings = Settings()

print(settings.DATA_DIR)
con = connect(settings.DATA_DIR)

con.execute(
    """
CREATE SCHEMA IF NOT EXISTS bronze;
DROP TABLE IF EXISTS bronze.transaction_raw;
CREATE TABLE bronze.transaction_raw AS
SELECT *
FROM read_csv_auto(
    'lethe/data/raw/financial_fraud_detection_dataset.csv',
    header=true
);
""",
)

print(
    con.execute(
        """
DESCRIBE bronze.transaction_raw
""",
    ).fetchall(),
)


con.close()
