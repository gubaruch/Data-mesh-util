import unittest
import warnings
import logging
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/lib"))

from data_mesh_util import DataMeshAdmin as dmu

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

MESH_ACCOUNT = '887210671223'


class DataMeshAdminAccountTests(unittest.TestCase):
    _mgr = None
    _logger = logging.getLogger("DataMeshAdmin")

    @classmethod
    def setUpClass(cls):
        cls._mgr = dmu.DataMeshAdmin(data_mesh_account_id=MESH_ACCOUNT, region_name='eu-west-1',
                                     log_level=logging.DEBUG)

    def test_setup_data_mesh_account(self):
        output = self._mgr.initialize_mesh_account()
        self._logger.info(output)

    def test_grant_producer_access(self):
        self._mgr.initialize_producer_account()

    def test_setup_consumer(self):
        self._mgr.initialize_consumer_account()
