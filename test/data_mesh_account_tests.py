import unittest
import warnings
import logging
import sys
import os
import data_mesh_util.lib.utils as utils
from data_mesh_util.lib.constants import *

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/lib"))

from data_mesh_util import DataMeshAdmin as dmu

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

MESH_ACCOUNT = '887210671223'


class DataMeshAdminAccountTests(unittest.TestCase):
    _logger = logging.getLogger("DataMeshAdmin")
    _client, _account_ids, _creds = utils.load_client_info_from_file(from_path=os.getenv('CredentialsFile'),
                                                                     region_name=os.getenv('AWS_REGION'))

    def test_setup_data_mesh_account(self):
        mgr = dmu.DataMeshAdmin(data_mesh_account_id=MESH_ACCOUNT, region_name='eu-west-1',
                                log_level=logging.DEBUG, use_creds=self._creds.get(MESH))
        output = self._mgr.initialize_mesh_account()
        self._logger.info(output)

    def test_grant_producer_access(self):
        creds = os.getenv('AWS_ACCESS_KEY_ID')
        if creds is None:
            creds = self._creds.get(PRODUCER)
        else:
            creds = None

        mgr = dmu.DataMeshAdmin(data_mesh_account_id=MESH_ACCOUNT, region_name='eu-west-1',
                                log_level=logging.DEBUG, use_creds=creds)
        mgr.initialize_producer_account()

        # now flip over to the mesh account and enable the producer
        mgr = dmu.DataMeshAdmin(data_mesh_account_id=MESH_ACCOUNT, region_name='eu-west-1',
                                log_level=logging.DEBUG, use_creds=self._creds.get(MESH))
        mgr.enable_account_as_producer(self._account_ids.get(PRODUCER))

    def test_setup_consumer(self):
        creds = os.getenv('AWS_ACCESS_KEY_ID')
        if creds is None:
            creds = self._creds.get(PRODUCER)
        else:
            creds = None

        mgr = dmu.DataMeshAdmin(data_mesh_account_id=MESH_ACCOUNT, region_name='eu-west-1',
                                log_level=logging.DEBUG, use_creds=creds)
        mgr.initialize_consumer_account()

        # now flip over to the mesh account and enable the consumer
        mgr = dmu.DataMeshAdmin(data_mesh_account_id=MESH_ACCOUNT, region_name='eu-west-1',
                                log_level=logging.DEBUG, use_creds=self._creds.get(MESH))
        mgr.enable_account_as_consumer(self._account_ids.get(CONSUMER))
