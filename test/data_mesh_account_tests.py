import unittest
from data_mesh_util import DataMeshAdmin as dmu
import warnings
from data_mesh_util.lib.constants import *

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)


class DataMeshAdminAccountTests(unittest.TestCase):
    _mgr = None

    @classmethod
    def setUpClass(cls):
        cls._mgr = dmu.DataMeshAdmin()

    def test_setup_data_mesh_account(self):
        self._mgr.initialize_mesh_account()

    def test_grant_producer_access(self):
        self._mgr.enable_account_as_producer(account_id='600214582022')
