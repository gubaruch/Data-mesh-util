import json
import logging
import unittest
import sys
import os
import warnings
import boto3
import test_utils as test_utils

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/lib"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../test"))

from data_mesh_util import DataMeshProducer as dmp
from data_mesh_util import DataMeshConsumer as dmc
from data_mesh_util.lib.SubscriberTracker import *


class DataMeshIntegrationTests(unittest.TestCase):
    _log_level = "DEBUG"

    # load credentials
    _region, _client, _account_ids, _creds = test_utils.load_client_info_from_file(
        from_path=os.getenv('CredentialsFile')
    )

    # create a subscriber tracker that is bound into the Mesh account, that will help us to inspect what's happening behind the scenes
    _subscription_tracker = SubscriberTracker(credentials=boto3.session.Session().get_credentials(),
                                              data_mesh_account_id=_account_ids.get('Mesh'),
                                              region_name=_region,
                                              log_level=_log_level)

    def get_role_credentials(self, input_creds, assume_role_name: str):
        sts_client = utils.generate_client('sts', region='us-east-1', credentials=input_creds)
        current_account = sts_client.get_caller_identity()
        session_name = utils.make_iam_session_name(current_account)
        assumed_role = sts_client.assume_role(
            RoleArn=utils.get_role_arn(
                account_id=current_account.get('Account'), role_name=assume_role_name),
            RoleSessionName=session_name
        )
        return assumed_role.get('Credentials')

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)

        # create a data producer class in the Producer Account
        producer_credentials = self.get_role_credentials(
            input_creds=self._creds.get(PRODUCER),
            assume_role_name=DATA_MESH_PRODUCER_ROLENAME
        )
        self._producer = dmp.DataMeshProducer(data_mesh_account_id=self._account_ids.get(MESH),
                                              region_name=self._region,
                                              use_credentials=producer_credentials)

        # create a data consumer class in the Consumer Account
        consumer_credentials = self.get_role_credentials(
            input_creds=self._creds.get(CONSUMER),
            assume_role_name=DATA_MESH_CONSUMER_ROLENAME
        )
        self._consumer = dmc.DataMeshConsumer(data_mesh_account_id=self._account_ids.get(MESH),
                                              region_name=self._region,
                                              use_credentials=consumer_credentials)

    def integration_test(self):
        db = 'tpcds'
        t = 'customer'
        # create a data product
        self._producer.create_data_products(
            source_database_name=db,
            table_name_regex=t
        )

        # make sure we can get it back
        data_product = self._producer.get_data_product(database_name=db, table_name_regex=t)
        self.assertIsNotNone(data_product)
        self.assertEqual(len(data_product), 1)

        # request access from the consumer
        requested_subscription = self._consumer.request_access_to_product(
            owner_account_id=self._account_ids.get('Producer'),
            database_name=f"{db}-{self._account_ids.get(PRODUCER)}",
            tables=[t], request_permissions=['SELECT']
        )

        # verify that we can retrieve the subscription
        subscription = self._consumer.get_subscription(request_id=requested_subscription.get("SubscriptionId"))
        self.assertIsNotNone(subscription)
        self.assertEqual(subscription.get('Status'), STATUS_PENDING)

        # approve access from the producer
        approval = self._producer.approve_access_request(
            request_id=requested_subscription.get("SubscriptionId"),
            grant_permissions=requested_subscription.get("RequestedGrants"),
            grantable_permissions=None, decision_notes='Approved'
        )

        # finalize the subscription
        self._consumer.finalize_subscription(subscription_id=requested_subscription.get("SubscriptionId"))

        # confirm that the consumer can see that it's status is now Active
        subscription = self._consumer.get_subscription(request_id=requested_subscription.get("SubscriptionId"))
        self.assertIsNotNone(subscription)
        self.assertEqual(subscription.get('Status'), STATUS_ACTIVE)

        # tear down the subscription

        # delete the data product
