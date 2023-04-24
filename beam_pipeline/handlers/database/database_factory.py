from abc import ABC

from handlers.data_struct.dvpd import Dvpd
from handlers.database.database_handler import DatabaseHandler
from handlers.database.gcp_handler import GcpHandler


class DataBaseHandlerFactory(ABC):
    def get_db_handler(self, dvpd: Dvpd) -> DatabaseHandler:
        """returns a new db handler instance"""


class GcpFactory(DataBaseHandlerFactory):
    def get_db_handler(self, dvpd: Dvpd) -> DatabaseHandler:
        return GcpHandler(dvpd=dvpd)
