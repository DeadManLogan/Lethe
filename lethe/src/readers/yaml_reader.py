import logging
import yaml
from pathlib import Path

logger = logging.getLogger(__name__)

class YAMLReader:
    """Parse and manage schema contracts from YAML files"""
    def __init__(self, contract_path: str):
        self.contract_path = Path(contract_path)
        self.data = self._load_yaml()

        logger.info(f"Loaded schema contract: {self.contract_path.name}")

    def _load_yaml(self) -> dict:
        """Load YAML file"""
        with open(self.contract_path, 'r') as f:
            data = yaml.safe_load(f)

        if not data:
            raise ValueError(f"Schema contract is empty: {self.contract_path}")

        return data
    
    def get_source_schema(self) -> str:
        """Get source schema name"""
        return self.data['source_schema']
    
    def get_source_table(self) -> str:
        """Get source table name"""
        return self.data['source_table']
    
    def get_target_schema(self) -> str:
        """Get target schema name"""
        return self.data['target_schema']
    
    def get_target_table(self) -> str:
        """Get target table name"""
        return self.data['target_table']
    
    def get_table_name(self) -> str:
        """Get logical table name"""
        return self.data['table_name']