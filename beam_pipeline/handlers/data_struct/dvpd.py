from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, validator


class DatabaseEnum(str, Enum):
    bigquery = "bigquery"


class StereotypeEnum(str, Enum):
    hub = "hub"
    sat = "sat"
    link = "link"


class DV_Stereotype_Error(Exception):
    """Custom error if a non valid datavault stereotype was entered"""

    def __init__(self, value: str, message: str) -> None:
        self.value = value
        self.message = message
        super().__init__(message)


class DB_Type_Error(Exception):
    """Custom error if a non supported database was entered"""

    def __init__(self, value: str, message: str) -> None:
        self.value = value
        self.message = message
        super().__init__(message)


class DV_Object_Missing_Error(Exception):
    """Custom error if a DV Object is missing"""

    def __init__(self, value: str, message: str) -> None:
        self.value = value
        self.message = message
        super().__init__(message)


class DataExtraction(BaseModel):
    fetch_module_name: str
    increment_logic: str
    search_expression: str
    file_archive_path: str
    parse_module_name: str
    codepage: str
    columnseparator: str
    rowseparator: str
    skip_first_rows: int
    reject_procedure: str


class DataDestination(BaseModel):
    database: DatabaseEnum
    provider: str
    project: Optional[str]
    temp_file: Optional[str]


class DestinationTables(BaseModel):
    table_name: str


class DataFields(BaseModel):
    field_name: str
    technical_type: str
    targets: List[DestinationTables] = []


class DataVaultObject(BaseModel):
    table_name: str
    stereotype: StereotypeEnum
    key_column_name: str
    hkey: Optional[List[str]]
    parent_table: Optional[str]
    link_objects: Optional[List[str]]


class DataVaultModel(BaseModel):
    schema_name: str
    tables: List[DataVaultObject] = []

    @validator("tables")
    @classmethod
    def referenced_table_exists(
        cls, value: List[DataVaultObject], values: dict
    ) -> List[DataVaultObject]:
        """Check if all Satellite parent objects or objects needed for links are existing"""
        table_names = [dv_obj.dict()["table_name"] for dv_obj in value]
        tables_to_validate = [
            dv_obj.dict()["parent_table"]
            for dv_obj in value
            if dv_obj.dict()["parent_table"]
        ]

        linking_tables = [
            dv_obj.dict().get("link_objects")
            for dv_obj in value
            if dv_obj.dict().get("link_objects")
        ]
        linking_tables = [table for tables in linking_tables for table in tables]

        tables_to_validate.extend(linking_tables)
        table_names_set = set(table_names)
        if diff := [
            table for table in tables_to_validate if table not in table_names_set
        ]:
            raise DV_Object_Missing_Error(
                value=value, message=f"{diff} is not existing in Data Vault Model"
            )
        return value


class Dvpd(BaseModel):
    DVPD_Version: str
    pipeline_name: str
    record_source_name_expression: str
    data_extraction: DataExtraction
    data_destination: Optional[DataDestination]
    fields: List[DataFields]
    data_vault_model: List[DataVaultModel]

    @validator("data_vault_model")
    @classmethod
    def fields_valid(cls, value: DataVaultModel, values: dict) -> DataVaultModel:
        """Check if all dv_objects exist that were defined as data destination"""
        # get all defined data vault objects
        dv_obj = [
            table.get("table_name")
            for dv_model in value
            for table in dv_model.dict().get("tables")
        ]
        dv_obj = list(dict.fromkeys(dv_obj))
        # get all field destinations
        tables = [
            target.get("table_name")
            for field in values["fields"]
            for target in field.dict().get("targets")
        ]
        tables = list(dict.fromkeys(tables))
        # compare lists
        set_dv_obj = set(dv_obj)

        if diff := [table for table in tables if table not in set_dv_obj]:
            raise DV_Object_Missing_Error(
                value=value, message=f"{diff} are not defined in Data Vault Model"
            )

        return value

    def get_hkey_cols_for_table(self, table: str, result: List[str]) -> List[str]:
        """Recursive function, retrieving all HKEY_Columns for given Data Vault Object"""
        dv_models = self.dict(include={"data_vault_model"}).get("data_vault_model")
        for dv_model in dv_models:
            for tbl in dv_model["tables"]:
                if tbl["table_name"] == table:
                    if tbl["stereotype"] != "link":
                        result.append(tbl["key_column_name"])
                    else:
                        result.insert(0, tbl["key_column_name"])
                        for link_obj in tbl["link_objects"]:
                            result = self.get_hkey_cols_for_table(link_obj, result)
        return result

    def get_field_cols_for_table(self, table: str, result: List[str]) -> List[str]:
        """returns all fields for a table"""
        dvpd_fields = self.dict(include={"fields"}).get("fields")
        return [
            result.append(field["field_name"])
            for field in dvpd_fields
            for targets in field["targets"]
            if table in targets.values()
        ]

    def create_dv_obj_list(self) -> List[Dict]:
        """returns list of all dv_objects and their schema"""
        dv_models = self.dict(include={"data_vault_model"}).get("data_vault_model")
        return [
            {"schema": dv_model["schema_name"], "table": tbl["table_name"]}
            for dv_model in dv_models
            for tbl in dv_model["tables"]
        ]

    def get_table_schema(self, table: str) -> str:
        dv_models = self.dict(include={"data_vault_model"}).get("data_vault_model")
        return [
            dv_model["schema_name"]
            for dv_model in dv_models
            for tbl in dv_model["tables"]
            if tbl["table_name"] == table
        ][0]
