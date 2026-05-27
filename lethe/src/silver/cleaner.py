# src/cleaning/cleaner.py

import logging
import yaml
from pathlib import Path
from duckdb import DuckDBPyConnection
from lethe.src.readers.yaml_reader import YAMLReader

logger = logging.getLogger(__name__)


class DataCleaner:
    """Clean raw data: Bronze → Silver transformation"""
    
    def __init__(
        self,
        schema_contract_path: str = None
    ):
        """
        Initialize DataCleaner
        
        Args:
            schema_contract_path: Path to YAML schema contract
        """
        self.schema = YAMLReader(schema_contract_path)

        self.source_schema = self.schema.get_source_schema()
        self.source_table = self.schema.get_source_table()
        self.target_schema = self.schema.get_target_schema()
        self.target_table = self.schema.get_target_table()
        self.schema_contract_path = schema_contract_path
        
        # Load schema contract
        self.schema_contract = self._load_schema_contract()
        
        logger.info(
            "DataCleaner initialized: %s.%s → %s.%s",
            self.source_schema, self.source_table,
            self.target_schema, self.target_table
        )