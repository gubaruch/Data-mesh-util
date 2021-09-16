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
from data_mesh_util import DataMeshConsumer as dmc
from data_mesh_util.lib.SubscriberTracker import *

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

class DataMeshIntegrationTests(unittest.TestCase):
    _log_level = "DEBUG"
    _current_region = os.getenv('AWS_REGION')
    _subscription_tracker = SubscriberTracker(credentials=boto3.session.Session().get_credentials(),
                                              data_mesh_account_id=MESH_ACCOUNT,
                                              region_name=_current_region,
                                              log_level=_log_level)
    _sts_client = boto3.client('sts')
    _clients = {
        MESH_ACCOUNT: {

        },
        CONSUMER_ACCOUNT: {

        }
        PRODUCER_ACCOUNT: {

        }
    }
    def integration_test:
        pass
