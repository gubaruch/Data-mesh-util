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
        # create a data product
        self._producer.create_data_products(
            source_database_name='tpcds',
            table_name_regex='customer',
            sync_mesh_catalog_schedule="cron(0 */2 * * ? *)",
            sync_mesh_crawler_role_arn="arn:aws:iam::600214582022:role/service-role/AWSGlueServiceRole-Crawler"
        )
