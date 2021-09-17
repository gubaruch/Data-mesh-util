import json
import logging
import unittest
import sys
import os
import warnings
import boto3

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/lib"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../test"))

from data_mesh_util import DataMeshProducer as dmp
from data_mesh_util import DataMeshConsumer as dmc
from data_mesh_util.lib.SubscriberTracker import *


class DataMeshIntegrationTests(unittest.TestCase):
    _log_level = "DEBUG"
    _current_region = os.getenv('AWS_REGION')

    # load credentials
    _creds = None
    cred_file = os.getenv('CredentialsFile')
    with open(cred_file, 'r') as w:
        _creds = json.load(w)
        w.close()

    _clients = {}
    _account_ids = {}
    for token in ['Mesh', 'Producer', 'Consumer']:
        _clients[token] = utils.generate_client('sts', region=_current_region, credentials=_creds.get(token))
        _account_ids[token] = _creds.get(token).get('AccountId')

    _subscription_tracker = SubscriberTracker(credentials=boto3.session.Session().get_credentials(),
                                              data_mesh_account_id=_account_ids.get('Mesh'),
                                              region_name=_current_region,
                                              log_level=_log_level)

    _producer = dmp.DataMeshProducer(data_mesh_account_id=_account_ids.get('Mesh'),
                                     use_credentials=_creds.get('Producer'))
    _consumer = dmc.DataMeshConsumer(data_mesh_account_id=_account_ids.get('Mesh'),
                                     use_credentials=_creds.get('Consumer'))

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def integration_test(self):
        db = 'tpcds'
        t = 'customer'
        # create a data product
        self._producer.create_data_products(
            source_database_name=db,
            table_name_regex=t
        )

        data_product = self._producer.get_data_product(database_name=db, table_name_regex=t)
        self.assertIsNotNone(data_product)
        self.assertEqual(len(data_product), 1)

        # request access from the consumer
        requested_subscription = self._consumer.request_access_to_product(
            owner_account_id=self._account_ids.get('Producer'),
            database_name=db,
            tables=[t], request_permissions=['SELECT'],
            requesting_principal=self._account_ids.get(
                'Consumer'))
        subscription = self._consumer.get_subscription(request_id=requested_subscription.get("SubscriptionId"))
        self.assertIsNotNone(subscription)
        self.assertEqual(subscription.get('Status'), STATUS_PENDING)

        # approve access from the producer
        approval = self._producer.approve_access_request(
            request_id=requested_subscription.get("SubscriptionId"),
            grant_permissions=requested_subscription.get("RequestedGrants"),
            grantable_permissions=None, decision_notes='Approved'
        )
        subscription = self._consumer.get_subscription(request_id=requested_subscription.get("SubscriptionId"))
        self.assertIsNotNone(subscription)
        self.assertEqual(subscription.get('Status'), STATUS_ACTIVE)

        # tear down the subscription

        # delete the data product
