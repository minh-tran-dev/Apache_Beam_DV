from abc import ABC

from handlers.file.file_handler import FileHandler
from handlers.file.file_handler_gcp import FileHandlerGcp
from handlers.file.file_handler_local import LocalFileHandler


class FileHandlerFactory(ABC):
    def get_file_handler(self) -> FileHandler:
        """returns a new file handler instance"""


class LocalFileFactory(FileHandlerFactory):
    def get_file_handler(self) -> FileHandler:
        return LocalFileHandler()


class GcpFileFactory(FileHandlerFactory):
    def get_file_handler(self) -> FileHandler:
        return FileHandlerGcp()
