from lethe.src.bronze.ingestion import start_ingestion
from lethe.src.utils.logging_config import configure_logging
from lethe.src.silver.cleaner import DataCleaner
from lethe.src.readers.yaml_reader import YAMLReader
from lethe.src.database.connection import connect, disconnect
from lethe.src.database.db_helpers import drop_table

con = connect()

configure_logging()

start_ingestion(con)

# YamlReader
# yaml_reader = YAMLReader("lethe/src/config/schema_contracts/transactions.yaml")
# print(yaml_reader.get_column_types())

# cleaner = DataCleaner(
#     schema_contract_path='lethe/src/config/schema_contracts/transactions.yaml'
# )
# cleaner.cast_types(con)
disconnect(con)
