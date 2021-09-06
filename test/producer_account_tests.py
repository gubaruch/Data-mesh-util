import logging
import unittest
import sys
import os
import warnings
import boto3

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/lib"))

from data_mesh_util import DataMeshProducer as dmp
from data_mesh_util.lib.SubscriberTracker import *

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

MESH_ACCOUNT = '887210671223'
CONSUMER_ACCOUNT = '206160724517'
PRODUCER_ACCOUNT = '600214582022'


class DataMeshProducerAccountTests(unittest.TestCase):
    '''
    Class to test the functionality of a data producer. Should be run using credentials for a principal who can assume
    the DataMeshAdminProducer role in the data mesh. Requires environment variables:

    AWS_REGION
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_SESSION_TOKEN (Optional)
    '''
    # bind the test class into the data mesh account
    sts_client = boto3.client('sts')
    current_account = sts_client.get_caller_identity()
    session_name = utils.make_iam_session_name(current_account)
    _data_producer_role_arn = utils.get_datamesh_producer_role_arn(account_id=MESH_ACCOUNT)
    _data_mesh_sts_session = sts_client.assume_role(RoleArn=_data_producer_role_arn,
                                                    RoleSessionName=session_name)

    _mgr = dmp.DataMeshProducer(data_mesh_account_id=MESH_ACCOUNT, log_level=logging.DEBUG)
    _current_region = os.getenv('AWS_REGION')
    _subscription_tracker = SubscriberTracker(data_mesh_account_id=MESH_ACCOUNT,
                                              credentials=_data_mesh_sts_session.get('Credentials'),
                                              region_name=_current_region,
                                              log_level=logging.DEBUG)

    def test_create_data_product(self):
        self._mgr.create_data_products(
            data_mesh_account_id=MESH_ACCOUNT,
            source_database_name='tpcds',
            table_name_regex='customer',
            sync_mesh_catalog_schedule="cron(0 */2 * * ? *)",
            sync_mesh_crawler_role_arn="arn:aws:iam::600214582022:role/service-role/AWSGlueServiceRole-Crawler"
        )

    def test_subscription_lifecycle(self):
        # create a subscription
        set_grants = ['DESCRIBE', 'SELECT']
        subscription_id = self._subscription_tracker.create_subscription_request(
            owner_account_id=MESH_ACCOUNT,
            database_name="Test",
            tables=["test"],
            principal=CONSUMER_ACCOUNT,
            request_grants=set_grants,
            suppress_object_validation=True
        )[0].get("SubscriptionId")

        # status should be Pending
        sub = self._subscription_tracker.get_subscription(subscription_id=subscription_id)
        self.assertEqual(sub.get(STATUS), STATUS_PENDING)

        # now update the status to approved
        self._mgr.approve_access_request(request_id=subscription_id, decision_notes='OK')

        # status should be Pending, notes should match, and permissions should be as requested
        sub = self._subscription_tracker.get_subscription(subscription_id=subscription_id)
        self.assertEqual(sub.get(STATUS), STATUS_ACTIVE)
        self.assertEqual(sub.get(NOTES), {'OK'})
        self.assertListEqual(sub.get(PERMITTED_GRANTS), set_grants)

        # add some more grants
        new_grants = ['SELECT', 'DESCRIBE', 'INSERT']
        self._subscription_tracker.update_grants(subscription_id=subscription_id, permitted_grants=new_grants,
                                                 notes="Yolo")
        sub = self._subscription_tracker.get_subscription(subscription_id=subscription_id)
        self.assertListEqual(sub.get(PERMITTED_GRANTS), new_grants)
