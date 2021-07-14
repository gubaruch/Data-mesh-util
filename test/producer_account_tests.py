import unittest
from data_mesh_util import DataMeshProducer as dmp
import warnings

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

PRODUCER_ROLE_ARN = "arn:aws:iam::887210671223:role/AwsDataMesh/DataMeshAdminProducer"


class DataMeshProducerAccountTests(unittest.TestCase):
    mgr = dmp.DataMeshProducer()

    def test_setup_producer_iam_role(self):
        self.mgr.initialize_producer_account(
            s3_bucket="org-1-data",
            data_mesh_producer_role_arn=PRODUCER_ROLE_ARN
        )

    def test_enable_future_sharing(self):
        self.mgr.enable_future_sharing("blah/blah")

    def test_grant_bucket_access(self):
        self.mgr.grant_datamesh_access_to_s3(
            s3_bucket="org-1-data",
            data_mesh_account_id="887210671223"
        )

    def test_create_data_product(self):
        self.mgr.create_data_product(
            data_mesh_producer_role_arn=PRODUCER_ROLE_ARN,
            source_database_name='tpcds',
            table_name_regex='customer'
        )
