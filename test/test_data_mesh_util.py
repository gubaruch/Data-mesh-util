import unittest
from data_mesh_util import DataMeshManager as dmu
import warnings

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

class DataMeshUtilTests(unittest.TestCase):
    def test_data_mesh_iam_role(self):
        mgr = dmu.DataMeshManager()
        mgr.initialize_mesh_account()
