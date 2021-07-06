import unittest
from data_mesh_util import DataMeshManager as dmu
import warnings

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

class DataMeshProducerAccountTests(unittest.TestCase):
    mgr = dmu.DataMeshManager()

    def test_producer_iam_role(self):
        self.mgr.initialize_producer_account(s3_bucket="org-1-data/tpcds")

    def test_enable_bucket_access(self):
        self.mgr.enable_future_bucket_sharing("blah/blah")