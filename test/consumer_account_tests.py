import unittest
import warnings
import logging
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/lib"))

from data_mesh_util import DataMeshConsumer as dmc

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

MESH_ACCOUNT = '887210671223'
CONSUMER_ACCOUNT = '206160724517'
PRODUCER_ACCOUNT = '600214582022'
DATABASE_NAME = "tpcds-%s" % PRODUCER_ACCOUNT


class ConsumerAccountTests(unittest.TestCase):
    '''
    Tests for consumer functionality including creating subscriptions, both positive and negative cases, as well as retirement.
    Should be run using credentials for a principal who can assume
    the DataMeshAdminConsumer role in the data mesh. Requires environment variables:

    AWS_REGION
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_SESSION_TOKEN (Optional)
    '''
    _mgr = dmc.DataMeshConsumer(data_mesh_account_id=MESH_ACCOUNT, log_level=logging.DEBUG)
    _logger = logging.getLogger("DataMeshConsumer")

    def test_create_subscription(self):
        sub = self._mgr.request_access_to_product(
            owner_account_id=PRODUCER_ACCOUNT,
            database_name=DATABASE_NAME,
            request_permissions=['SELECT', 'DESCRIBE'],
            tables=['customer'],
            requesting_principal=CONSUMER_ACCOUNT
        )
        self.assertIsNotNone(sub)
        self._logger.info('Subscription %s' % sub)

        # now fetch the subscription
        sub_id = sub.get("SubscriptionId")
        subscription = self._mgr.get_subscription(sub_id)
        self.assertIsNotNone(subscription)
        self.assertEqual(subscription.get("SubscriptionId"), sub_id)

    def test_fail_create_subscription(self):
        with self.assertRaises(Exception):
            sub = self._mgr.request_access_to_product(
                owner_account_id=PRODUCER_ACCOUNT,
                database_name=DATABASE_NAME,
                request_permissions=['SELECT', 'DESCRIBE'],
                tables=['does_not_exist'],
                requesting_principal=CONSUMER_ACCOUNT
            )
