from lethe.src.bronze.ingestion import start_ingestion
from lethe.src.utils.logging_config import configure_logging
from lethe.src.silver.cleaner import DataCleaner
from lethe.src.readers.yaml_reader import YAMLReader

configure_logging()

start_ingestion()

# YamlReader
yaml_reader = YAMLReader("lethe/src/config/schema_contracts/transactions.yml")


# cleaner = DataCleaner(
#     source_schema='raw',
#     source_table='transactions',
#     target_schema='cleaned',
#     schema_contract_path='lethe/src/config/schema_contracts/transactions.yml'
# )
# print(cleaner.get_info())
