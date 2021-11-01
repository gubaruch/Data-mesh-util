import unittest
import warnings
import logging
import sys
import os
import test_utils
from data_mesh_util.lib.constants import *

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/lib"))
import data_mesh_util.lib.utils as utils
from data_mesh_util import DataMeshConsumer as dmc

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)


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
    _logger = logging.getLogger("DataMeshConsumer")
    _region, _clients, _account_ids, _creds = test_utils.load_client_info_from_file(
        from_path=os.getenv('CredentialsFile'))

    # bind the test class into the consumer account
    _sts_session = test_utils.assume_source_role(sts_client=_clients.get(CONSUMER),
                                                 account_id=_account_ids.get(CONSUMER),
                                                 type=CONSUMER)
    consumer_credentials = _sts_session.get('Credentials')
    _sts_client = utils.generate_client('sts', _region, consumer_credentials)

    _mgr = dmc.DataMeshConsumer(data_mesh_account_id=_account_ids.get(MESH),
                                log_level=logging.DEBUG,
                                region_name=_region,
                                use_credentials=consumer_credentials)

    DATABASE_NAME = "tpcds-%s" % _account_ids.get(PRODUCER)

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_create_subscription(self):
        sub = self._mgr.request_access_to_product(
            owner_account_id=self._account_ids.get(PRODUCER),
            database_name=self.DATABASE_NAME,
            tables=['customer'],
            request_permissions=['SELECT', 'DESCRIBE'],
            requesting_principal=self._account_ids.get(CONSUMER)
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
                owner_account_id=self._account_ids.get(PRODUCER),
                database_name=self.DATABASE_NAME,
                request_permissions=['SELECT', 'DESCRIBE'],
                tables=['does_not_exist'],
                requesting_principal=self._account_ids.get(CONSUMER)
            )
