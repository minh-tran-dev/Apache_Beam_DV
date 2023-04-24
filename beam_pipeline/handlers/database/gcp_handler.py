import json
from pathlib import Path
from typing import Dict, List

from handlers.data_struct.dvpd import Dvpd
from handlers.database.database_handler import DatabaseHandler
from managers.gcp_manager import GCP_Manager


class GcpHandler(DatabaseHandler):
    def __init__(self, dvpd: Dvpd) -> None:
        super().__init__(dvpd=dvpd)
        self.gcp_mngr = GCP_Manager()

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def create_table(self, table_id: str, schema: Dict, delete: bool = False) -> None:
        self.gcp_mngr.create_table(table_id=table_id, schema=schema, delete=delete)

    def _get_field_ddl(self, table: str) -> List[Dict]:
        """create ein schema for one table defined in dvpd
        for Bigquery schema
        mode and description are not defined in imdb.json but we can expand it as needed
        """
        fields = self.dvpd.dict(include={"fields"}).get("fields")
        ddl = []
        for field in fields:
            for target in field.get("targets"):
                if target.get("table_name") == table:
                    field_schema = dict(
                        name=field.get("field_name"),
                        type=field.get("technical_type"),
                        mode=field.get("mode")
                        if "mode" in field.keys()
                        else "NULLABLE",
                        description=field.get("description")
                        if "description" in field.keys()
                        else "",
                    )
                    ddl.append(field_schema)
        return ddl

    def _add_technical_fields(self, table: str, field_ddl: List[Dict]) -> List[Dict]:
        ddl_list = [self._get_hkey_ddl(table=table)]
        if "link" in table:
            links = self._get_link_hkeys(table)
            ddl_list += links
        ddl_list += field_ddl
        return ddl_list

    def _add_metadata_ddl(self, table: str, tech_fields: Dict) -> List[Dict]:
        ddl_list = []
        for k, v in tech_fields.items():
            if k == "hdiff" and "sat" not in table:
                continue
            tech_field = dict(name=v.get("name"), type=v.get("type"), mode="NULLABLE")
            ddl_list.append(tech_field)
        return ddl_list

    def _get_hkey_ddl(self, table: str) -> Dict:
        dvpd_dv_model = self.dvpd.dict(include={"data_vault_model"}).get(
            "data_vault_model"
        )
        key_column_name = [
            tbl.get("key_column_name")
            for dv_model in dvpd_dv_model
            for tbl in dv_model.get("tables")
            if table == tbl.get("table_name")
        ]
        used_fields = [
            tbl.get(list(tbl)[3])
            for dv_model in dvpd_dv_model
            for tbl in dv_model.get("tables")
            if table == tbl.get("table_name")
        ]
        return dict(
            name=key_column_name[0],
            type="STRING",
            mode="NULLABLE",
            description=f"Parent_table : {table} || BK : {used_fields[0]}",
        )

    def _get_link_hkeys(self, table: str) -> List[Dict]:
        dvpd_dv_model = self.dvpd.dict(include={"data_vault_model"}).get(
            "data_vault_model"
        )
        used_fields = [
            tbl.get("link_objects")
            for dv_model in dvpd_dv_model
            for tbl in dv_model.get("tables")
            if table == tbl.get("table_name")
        ]
        return [self._get_hkey_ddl(link) for link in used_fields[0]]

    def _get_db_schema(self, table: str) -> str:
        dvpd_dv_model = self.dvpd.dict(include={"data_vault_model"}).get(
            "data_vault_model"
        )
        return [
            dv_model.get("schema_name")
            for dv_model in dvpd_dv_model
            for tbl in dv_model.get("tables")
            if table == tbl.get("table_name")
        ]

    def _write_schema_to_local(
        self, schema: dict, db_schema: str, table_name: str, file_dir: Path
    ) -> None:
        if not schema:
            return
        file_path = (
            file_dir
            / f"{self.dvpd.data_destination.project}.{db_schema}.{table_name}.json"
        )
        with file_path.open("w", encoding="UTF-8") as schema_file:
            json.dump(schema, schema_file)

    def _write_ddl_to_bucket(
        self, ddl: List[dict], db_schema: str, table_name: str, bucket: str
    ) -> None:
        if not ddl:
            return
        self.gcp_mngr.upload_ddl(
            bucket_name=bucket,
            ddl=ddl,
            file_name=f"ddl/{self.dvpd.data_destination.project}.{db_schema}.{table_name}.json",
        )

    def create_ddls(self, tech_fields: Dict) -> None:
        self.logger.info("Creating BQ schemas")

        tables = self.get_tables()
        # local_out_dir = Path(__file__).parent.parent.parent / 'output'

        for table in tables:
            db_schema = self._get_db_schema(table=table)
            field_ddl = self._get_field_ddl(table=table)
            field_ddl = self._add_technical_fields(table=table, field_ddl=field_ddl)
            field_ddl += self._add_metadata_ddl(table=table, tech_fields=tech_fields)
            for schema in db_schema:
                # self._write_schema_to_local(schema=field_ddl,db_schema=schema,table_name=table,file_dir=local_out_dir)
                self._write_ddl_to_bucket(
                    ddl=field_ddl,
                    db_schema=schema,
                    table_name=table,
                    bucket=f"{self.dvpd.data_destination.project}-resources",
                )

        self.logger.info("------ Finished creating ddls ------")

    def create_tables(self, partition: str, delete: bool) -> None:
        self.logger.info("------ start creating tables ------")

        self.gcp_mngr.create_table_from_file(
            bucket_name=f"{self.dvpd.data_destination.project}-resources",
            partition=partition,
            delete=delete,
        )

        self.logger.info("------ Finished creating tables ------")

    def test(self):
        return super().test()
