import logging
import yaml
from pathlib import Path
from duckdb import DuckDBPyConnection
from lethe.src.readers.yaml_reader import YAMLReader
from lethe.src.database.db_helpers import get_columns, get_row_count

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
        self.yaml_reader = YAMLReader(schema_contract_path)

        self.source_schema = self.yaml_reader.get_source_schema()
        self.source_table = self.yaml_reader.get_source_table()
        self.target_schema = self.yaml_reader.get_target_schema()
        self.target_table = self.yaml_reader.get_target_table()
        
        logger.info(
            "DataCleaner initialized: %s.%s → %s.%s",
            self.source_schema, self.source_table,
            self.target_schema, self.target_table
        )

    def cast_types(self, con:DuckDBPyConnection):
        """
        Cast all columns to types defined in silver schema contract.

        Read the types from YAML config and cast each column
        using TRY_CAST to handle invalid values.
        """
        logger.info(f"Casting column types for {self.source_schema}.{self.source_table}")

        yaml_column_types = self.yaml_reader.get_column_types()

        logger.info(f"YAML COLUMN DETAILS {yaml_column_types}")

        table_columns = get_columns(con, self.source_schema, self.source_table)
        logger.info(f"TABLE COLUMN DETAILS {table_columns}")
        table_column_names = [col[0] for col in table_columns]
        logger.info(f"TABLE COLUMN names {table_columns}")


        # cast_expression = []
        # for col_name in table_column_names:
        #     if col_name in yaml_column_types:
        #         target_type = yaml_column_types[col_name]
        #         cast_expression.append(
        #             f"TRY_CAST({col_name} AS {target_type}) AS {col_name}"
        #         )
        #         logger.debug(f"Casting {col_name} → {target_type}")
        #     else:
        #         logger.warning(f"Column {col_name} not in contract, keeping as is")
        #         cast_expression.append(col_name)

        # con.execute(f"""
        #     CREATE OR REPLACE TABLE {self.target_schema}.{self.target_table} AS
        #     SELECT {', '.join(cast_expression)}
        #     FROM {self.source_schema}.{self.source_table}
        # """)
        
        # row_count = get_row_count(con, self.target_schema, self.target_table)
        
        # logger.info(
        #     f"""Type casting complete: {row_count} rows written 
        #     to {self.target_schema}.{self.target_table}"""
        # )