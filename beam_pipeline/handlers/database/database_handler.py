import logging
from abc import ABC, abstractmethod
from typing import Dict

from handlers.data_struct.dvpd import Dvpd


class DatabaseHandler(ABC):
    def __init__(self, dvpd: Dvpd) -> None:
        self.dvpd = dvpd
        self.logger = logging.getLogger(__name__)

    def __enter__(self) -> None:
        self.connect

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.disconnect

    @abstractmethod
    def connect(self) -> None:
        """connect to target database"""

    @abstractmethod
    def disconnect(self) -> None:
        """disconnect to target database"""

    @abstractmethod
    def create_table(self, table_id: str, schema: Dict, delete: bool = False) -> None:
        """create a table in target database"""

    @abstractmethod
    def create_ddls(self, tech_fields: Dict) -> None:
        """create ddl per table"""

    @abstractmethod
    def create_tables(self, partition: str, delete: bool) -> None:
        """create actual tables in dabase from saved ddl files"""

    @abstractmethod
    def test(self):
        """testing purposes"""

    def get_tables(self) -> list:
        """get all tables defined in dvpd"""
        dv_models = self.dvpd.dict(include={"data_vault_model"}).get("data_vault_model")
        tables = [
            table["table_name"]
            for dv_model in dv_models
            for table in dv_model["tables"]
        ]
        tables = sorted(dict.fromkeys(tables))
        return tables

    def get_dv_type(self, target_table: str) -> str:
        """get stereotype of target table"""
        dv_models = self.dvpd.dict(include={"data_vault_model"}).get("data_vault_model")
        return [
            table["stereotype"]
            for dv_model in dv_models
            for table in dv_model["tables"]
            if table["table_name"] == target_table
        ]
