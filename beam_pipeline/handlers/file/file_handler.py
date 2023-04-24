import logging
from abc import ABC, abstractmethod
from typing import Dict

from handlers.data_struct.dvpd import Dvpd


class FileHandler(ABC):
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.initialize()

    def __enter__(self) -> None:
        pass

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass

    @abstractmethod
    def initialize(self):
        """Instantiate necessary components"""

    @abstractmethod
    def read_dvpd(self, dvpd_name: str, path: str) -> Dvpd:
        """returns dvpd"""

    @abstractmethod
    def get_pipeline_conf(self, path: str) -> Dict:
        """returns pipeline conf"""

    @abstractmethod
    def get_conf_file(self, path: str, config_name: str) -> Dict:
        """returns pipeline conf"""

    @abstractmethod
    def get_file_path(self, dvpd: Dvpd, pipeline_resource: str = None) -> str:
        """returns valid filepath for source data"""
