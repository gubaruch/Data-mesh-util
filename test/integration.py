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

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)


class DataMeshIntegrationTests(unittest.TestCase):
    _log_level = "DEBUG"
    _current_region = os.getenv('AWS_REGION')

    # load credentials
    _creds = None
    with open('integration-test-creds.json', 'r') as w:
        _creds = json.load(w)

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
        self._consumer.request_access_to_product(owner_account_id=self._account_ids.get('Producer'), database_name=db,
                                                 tables=[t], request_permissions=['SELECT'],
                                                 requesting_principal=self._account_ids.get('Consumer'))

        # approve access from the producer

        # tear down the subscription

        # delete the data product
