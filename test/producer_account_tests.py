import unittest
from data_mesh_util import DataMeshProducer as dmp
import warnings
import logging

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

MESH_ACCOUNT = '887210671223'


class DataMeshProducerAccountTests(unittest.TestCase):
    mgr = dmp.DataMeshProducer(log_level=logging.DEBUG)

    def test_setup_producer_iam_role(self):
        self.mgr.initialize_producer_account(
            s3_bucket="org-1-data",
            data_mesh_account_id=MESH_ACCOUNT
        )

    def test_enable_future_sharing(self):
        self.mgr.enable_future_sharing("blah/blah")

    def test_grant_bucket_access(self):
        self.mgr.grant_datamesh_access_to_s3(
            s3_bucket="org-1-data",
            data_mesh_account_id="887210671223"
        )

    def test_create_data_product(self):
        self.mgr.create_data_products(
            data_mesh_account_id=MESH_ACCOUNT,
            source_database_name='tpcds',
            table_name_regex='customer',
            sync_mesh_catalog_schedule="cron(0 */2 * * ? *)",
            sync_mesh_crawler_role_arn="arn:aws:iam::600214582022:role/service-role/AWSGlueServiceRole-Crawler"
        )
