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

from data_mesh_util import DataMeshAdmin as dmu

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

MESH_ACCOUNT = '887210671223'


class DataMeshAdminAccountTests(unittest.TestCase):
    _logger = logging.getLogger("DataMeshAdmin")
    _region, _clients, _account_ids, _creds = test_utils.load_client_info_from_file(
        from_path=os.getenv('CredentialsFile'))

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_setup_data_mesh_account(self):
        mgr = dmu.DataMeshAdmin(data_mesh_account_id=MESH_ACCOUNT, region_name=self._region,
                                log_level=logging.DEBUG, use_creds=self._creds.get(MESH))
        output = mgr.initialize_mesh_account()
        self._logger.info(output)

    def test_setup_producer(self):
        # enable the producer in the mesh account
        mesh_admin = dmu.DataMeshAdmin(data_mesh_account_id=MESH_ACCOUNT, region_name=self._region,
                                       log_level=logging.DEBUG, use_creds=self._creds.get(MESH))
        mesh_admin.enable_account_as_producer(self._account_ids.get(PRODUCER))

        # in the producer account, we'll initialize the required objects
        producer_admin = dmu.DataMeshAdmin(data_mesh_account_id=MESH_ACCOUNT, region_name=self._region,
                                           log_level=logging.DEBUG, use_creds=self._creds.get(PRODUCER_ADMIN))
        producer_admin.initialize_producer_account()

    def test_setup_consumer(self):
        # enable the consumer in the mesh account
        mesh_admin = dmu.DataMeshAdmin(data_mesh_account_id=MESH_ACCOUNT, region_name=self._region,
                                       log_level=logging.DEBUG, use_creds=self._creds.get(MESH))
        mesh_admin.enable_account_as_consumer(self._account_ids.get(CONSUMER))

        # in the consumer account, we'll initialize the required objects
        consumer_admin = dmu.DataMeshAdmin(data_mesh_account_id=MESH_ACCOUNT, region_name=self._region,
                                           log_level=logging.DEBUG, use_creds=self._creds.get(CONSUMER_ADMIN))
        consumer_admin.initialize_consumer_account()
