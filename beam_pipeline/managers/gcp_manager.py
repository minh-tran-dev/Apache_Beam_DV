import json
import logging
import tempfile
from dataclasses import dataclass, field
from typing import List

from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound


@dataclass
class GCP_Manager:
    _client: bigquery.Client = field(init=False, repr=False, default=None)
    _storage_client: storage.Client = field(init=False, repr=False, default=None)

    def __post_init__(self) -> None:
        self._client = bigquery.Client()
        self._storage_client = storage.Client()
        self.logger = logging.getLogger(__name__)

    def read_json_from_bucket(self, bucket_name: str, file_name: str) -> dict:
        bucket = self._storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        return json.loads(blob.download_as_string())

    def create_dataset(self, dataset_id: str, location: str = "EU") -> None:
        dataset = bigquery.Dataset(f"{dataset_id}")
        dataset.location = location
        dataset = self._client.create_dataset(dataset, timeout=30)
        self.logger.info(f"Created dataset {self._client.project}.{dataset.dataset_id}")

    def check_dataset_exists(self, dataset_id: str) -> bool:
        try:
            self._client.get_dataset(dataset_id)
            self.logger.info(f"Dataset {dataset_id} already exists")
            return True
        except NotFound:
            self.logger.info(f"Dataset {dataset_id} is not found")
            return False

    def check_table_exists(self, table_id: str) -> bool:
        try:
            self._client.get_table(table_id)
            self.logger.info(f"Table {table_id} already exists")
            return True
        except NotFound:
            self.logger.info(f"Table {table_id} is not found")
            return False

    def create_table(
        self, table_id: str, schema: dict, partition: str = None, delete: bool = False
    ) -> None:
        """table_id = project+dataset_id+table_id"""

        # check dataset
        split_table_id = table_id.split(".")
        dataset_id = f"{split_table_id[0]}.{split_table_id[1]}"
        if not self.check_dataset_exists(dataset_id):
            self.create_dataset(dataset_id)
        # delete table
        if delete:
            self._client.delete_table(table_id, not_found_ok=True)
            self.logger.info(f"Deleted table {table_id}")
        # no action on existing table
        if self.check_table_exists(table_id):
            return

        table = bigquery.Table(table_id, schema=schema)
        if partition:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field=partition
            )
        table = self._client.create_table(table)
        self.logger.info(
            f"Created table {table.project}.{table.dataset_id}.{table.table_id}"
        )

    def create_table_from_file(
        self, bucket_name: str, partition: str = None, delete: bool = False
    ) -> None:
        blobs = self._storage_client.list_blobs(bucket_name, prefix="ddl/")
        for blob in blobs:
            table_id = blob.name
            table_id = table_id.lstrip("ddl/")
            table_id = table_id.rstrip(".json")
            downloaded_blob = blob.download_as_text(encoding="utf-8")
            schema = json.loads(downloaded_blob)
            self.create_table(
                table_id=table_id, schema=schema, partition=partition, delete=delete
            )

    def get_schema_from_json_local(self, json_file) -> dict:
        return self._client.schema_from_json(json_file)

    def get_schema_from_json_cloud(self, bucket_name: str, file_name: str) -> dict:
        """file_name = folder_structure_in_bucket/file_name"""
        bucket = self._storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        with blob.open("r") as f:
            return self._client.schema_from_json(f)

    def upload_ddl(self, bucket_name: str, ddl: List[dict], file_name: str):
        bucket = self._storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(data=json.dumps(ddl), content_type="application/json")
        self.logger.info(f"Uploaded {file_name}")

    def get_blob(self, bucket_name: str, blob_name: str):
        bucket = self._storage_client.bucket(bucket_name)
        return bucket.blob(blob_name)
