import unittest
from data_mesh_util import DataMeshAdmin as dmu
import warnings

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)


class DataMeshAdminAccountTests(unittest.TestCase):
    def test_setup_data_mesh_account(self):
        mgr = dmu.DataMeshAdmin()
        mgr.initialize_mesh_account()
