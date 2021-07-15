import unittest
from data_mesh_util import DataMeshAdmin as dmu
import warnings
import logging

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)


class DataMeshAdminAccountTests(unittest.TestCase):
    _mgr = None
    _logger = logging.getLogger("DataMeshAdmin")

    @classmethod
    def setUpClass(cls):
        cls._mgr = dmu.DataMeshAdmin()

    def test_setup_data_mesh_account(self):
        output = self._mgr.initialize_mesh_account()
        self._logger.info(output)

    def test_grant_producer_access(self):
        self._mgr.enable_account_as_producer(account_id='600214582022')
