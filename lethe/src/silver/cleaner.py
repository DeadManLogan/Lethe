# src/cleaning/cleaner.py

import logging
import yaml
from pathlib import Path
from duckdb import DuckDBPyConnection

logger = logging.getLogger(__name__)


class DataCleaner:
    """Clean raw data: Bronze → Silver transformation"""
    
    def __init__(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str = None,
        schema_contract_path: str = None,
        on_schema_error: str = 'raise'
    ):
        """
        Initialize DataCleaner
        
        Args:
            source_schema: Source schema name (e.g., 'raw')
            source_table: Source table name (e.g., 'transactions')
            target_schema: Target schema name (e.g., 'cleaned')
            target_table: Target table name (defaults to source_table)
            schema_contract_path: Path to YAML schema contract
            on_schema_error: How to handle schema violations ('raise', 'drop', 'null')
        """
        self.source_schema = source_schema
        self.source_table = source_table
        self.target_schema = target_schema
        self.target_table = target_table or source_table
        self.on_schema_error = on_schema_error
        self.schema_contract_path = schema_contract_path
        
        # Load schema contract
        self.schema_contract = self._load_schema_contract()
        
        logger.info(
            "DataCleaner initialized: %s.%s → %s.%s",
            self.source_schema, self.source_table,
            self.target_schema, self.target_table
        )
    
    def _load_schema_contract(self) -> dict:
        """Load schema contract from YAML file"""
        if not self.schema_contract_path:
            logger.warning("No schema contract provided, using minimal defaults")
            return {}
        
        contract_path = Path(self.schema_contract_path)
        
        if not contract_path.exists():
            raise FileNotFoundError(f"Schema contract not found: {contract_path}")
        
        with open(contract_path, 'r') as f:
            contract = yaml.safe_load(f)
        
        logger.info("Loaded schema contract from: %s", contract_path)
        logger.debug("Contract contains %d columns", len(contract.get('columns', {})))
        
        return contract
    
    def get_info(self) -> dict:
        """Get information about the cleaner configuration"""
        return {
            'source': f"{self.source_schema}.{self.source_table}",
            'target': f"{self.target_schema}.{self.target_table}",
            'schema_contract_loaded': bool(self.schema_contract),
            'num_columns_in_contract': len(self.schema_contract.get('columns', {})),
            'on_schema_error': self.on_schema_error,
        }