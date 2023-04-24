import json
from pathlib import Path
from typing import Dict

from handlers.data_struct.dvpd import Dvpd
from handlers.file.file_handler import FileHandler


class LocalFileHandler(FileHandler):
    def __init__(self) -> None:
        super().__init__()
        self.resource_path = Path(__file__).parent.parent.parent.parent

    def initialize(self):
        return super().initialize()

    def read_dvpd(self, dvpd_name: str, path: str) -> Dvpd:
        data = self.get_dvpd_from_path(dvpd_name)
        return Dvpd(**data)

    def get_pipeline_conf(self, path: str) -> Dict:
        pipeline_conf_path = (
            self.resource_path / "resources" / "config" / "pipeline_conf.json"
        )
        with pipeline_conf_path.open("r") as conf:
            return json.load(conf)

    def get_conf_file(self, path: str, config_name: str) -> Dict:
        return Path(__file__).parent.parent.parent.parent / "config" / config_name

    def get_dvpd_from_path(self, dvpd_name: str) -> Dict:
        dvpd_path = self.resource_path / "resources" / "dvpd" / dvpd_name
        with dvpd_path.open("r") as source:
            self.logger.info(source)
            return json.load(source)

    def get_file_path(self, dvpd: Dvpd, pipeline_resource: str = None) -> str:
        return f"../{dvpd.data_extraction.file_archive_path}{dvpd.data_extraction.search_expression}"
