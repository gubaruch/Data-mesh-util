import unittest
import sys
import os
import warnings
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/lib"))

from data_mesh_util import DataMeshProducer as dmp

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

MESH_ACCOUNT = '887210671223'


class DataMeshProducerAccountTests(unittest.TestCase):
    mgr = dmp.DataMeshProducer(data_mesh_account_id=MESH_ACCOUNT, log_level=logging.DEBUG)

    def test_create_data_product(self):
        self.mgr.create_data_products(
            data_mesh_account_id=MESH_ACCOUNT,
            source_database_name='tpcds',
            table_name_regex='customer',
            sync_mesh_catalog_schedule="cron(0 */2 * * ? *)",
            sync_mesh_crawler_role_arn="arn:aws:iam::600214582022:role/service-role/AWSGlueServiceRole-Crawler"
        )
