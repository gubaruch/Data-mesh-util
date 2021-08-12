import unittest
import warnings
import logging
from data_mesh_util import DataMeshConsumer as dmc

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

MESH_ACCOUNT = '887210671223'
CONSUMER_ACCOUNT = '206160724517'
PRODUCER_ACCOUNT = '600214582022'
DATABASE_NAME = "tpcds-%s" % PRODUCER_ACCOUNT


class ConsumerAccountTests(unittest.TestCase):
    _mgr = dmc.DataMeshConsumer(data_mesh_account_id=MESH_ACCOUNT, log_level=logging.DEBUG)
    _logger = logging.getLogger("DataMeshConsumer")

    def test_create_subscription(self):
        sub = self._mgr.request_access_to_product(
            owner_account_id=PRODUCER_ACCOUNT,
            database_name=DATABASE_NAME,
            request_permissions=['SELECT', 'DESCRIBE'],
            tables=['1customer'],
            requesting_principal=CONSUMER_ACCOUNT
        )
        self.assertIsNotNone(sub)
        self._logger.info('Subscription %s' % sub)
