import tempfile
from typing import Dict

from handlers.data_struct.dvpd import Dvpd
from handlers.file.file_handler import FileHandler
from managers.gcp_manager import GCP_Manager


class FileHandlerGcp(FileHandler):
    def __init__(self) -> None:
        super().__init__()
        self.gcp_manager = GCP_Manager()

    def initialize(self):
        return super().initialize()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass

    def read_dvpd(self, dvpd_name: str, path: str) -> Dvpd:
        dvpd_obj = self.gcp_manager.read_json_from_bucket(
            bucket_name=path, file_name=f"dvpd/{dvpd_name}"
        )
        return Dvpd(
            **dvpd_obj
        )  # liest ein json object von der dvpd in ein Pydantic Modell

    def get_pipeline_conf(self, path: str) -> Dict:
        return self.gcp_manager.read_json_from_bucket(
            bucket_name=path, file_name=f"config/pipeline_conf.json"
        )

    def get_conf_file(self, path: str, config_name: str) -> Dict:
        return self.gcp_manager.get_blob(path, f"config/{config_name}")

    def get_file_path(self, dvpd: Dvpd, pipeline_resource: str = None) -> str:
        return (
            f"gs://{pipeline_resource}/input/{dvpd.data_extraction.search_expression}"
        )

    def test_gcp(self):
        pass
