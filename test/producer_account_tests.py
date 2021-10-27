import logging
import unittest
import sys
import os
import warnings
import boto3
import test_utils
from data_mesh_util.lib.constants import *

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/lib"))

from data_mesh_util import DataMeshProducer as dmp
from data_mesh_util.lib.SubscriberTracker import *

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)


class DataMeshProducerAccountTests(unittest.TestCase):
    '''
    Class to test the functionality of a data producer. Should be run using credentials for a principal who can assume
    the DataMeshAdminProducer role in the data mesh. Requires environment variables:

    AWS_REGION
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_SESSION_TOKEN (Optional)
    '''
    _region, _clients, _account_ids, _creds = test_utils.load_client_info_from_file(
        from_path=os.getenv('CredentialsFile'))

    # bind the test class into the data mesh account
    _sts_session = test_utils.assume_source_role(_clients.get(PRODUCER), _account_ids.get(PRODUCER), PRODUCER)
    _data_producer_role_arn = utils.get_datamesh_producer_role_arn(account_id=_account_ids.get(MESH))
    _sts_client = utils.generate_client('sts', _region, _sts_session.get('Credentials'))
    _session_name = utils.make_iam_session_name(_sts_client.get_caller_identity())
    _data_mesh_sts_session = _sts_client.assume_role(RoleArn=_data_producer_role_arn,
                                                     RoleSessionName=_session_name)

    _mgr = dmp.DataMeshProducer(data_mesh_account_id=_account_ids.get(MESH), log_level=logging.DEBUG)
    _current_region = os.getenv('AWS_REGION')
    _subscription_tracker = SubscriberTracker(data_mesh_account_id=_account_ids.get(MESH),
                                              credentials=_data_mesh_sts_session.get('Credentials'),
                                              region_name=_current_region,
                                              log_level=logging.DEBUG)

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_create_data_product(self):
        self._mgr.create_data_products(
            source_database_name='tpcds',
            table_name_regex='customer',
            sync_mesh_catalog_schedule="cron(0 */2 * * ? *)",
            sync_mesh_crawler_role_arn=f"arn:aws:iam::{self._account_ids.get(PRODUCER)}:role/service-role/AWSGlueServiceRole-Crawler"
        )
