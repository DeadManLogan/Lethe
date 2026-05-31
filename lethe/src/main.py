from lethe.src.bronze.ingestion import start_ingestion
from lethe.src.utils.logging_config import configure_logging
from lethe.src.silver.cleaner import DataCleaner
from lethe.src.readers.yaml_reader import YAMLReader

configure_logging()

start_ingestion()

# YamlReader
# yaml_reader = YAMLReader("lethe/src/config/schema_contracts/transactions.yaml")
# print(yaml_reader.get_column_types())


# cleaner = DataCleaner(
#     schema_contract_path='lethe/src/config/schema_contracts/transactions.yaml'
# )
