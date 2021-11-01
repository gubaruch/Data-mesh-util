import warnings
import logging
import os
import test.test_utils as test_utils
from data_mesh_util.lib.constants import *
from data_mesh_util import DataMeshAdmin as dmu

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

MESH_ACCOUNT = '887210671223'


class Step0():
    '''
    Script to configure an set of accounts as central data mesh, producer, and consumer. Mesh credentials must already
    have DataLakeAdmin permissions.
    '''
    _logger = logging.getLogger("DataMeshAdmin")
    _region, _clients, _account_ids, _creds = test_utils.load_client_info_from_file(
        from_path=os.getenv('CredentialsFile'))

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def setup_central_account(self):
        # create the data mesh
        mesh_admin = dmu.DataMeshAdmin(data_mesh_account_id=MESH_ACCOUNT, region_name=self._region,
                                       log_level=logging.DEBUG, use_creds=self._creds.get(MESH))
        mesh_admin.initialize_mesh_account()

        # create the producer account
        producer_admin = dmu.DataMeshAdmin(data_mesh_account_id=MESH_ACCOUNT, region_name=self._region,
                                           log_level=logging.DEBUG, use_creds=self._creds.get(PRODUCER_ADMIN))
        producer_admin.initialize_producer_account()
        mesh_admin.enable_account_as_producer(self._account_ids.get(PRODUCER))

        # create the consumer_account
        consumer_admin = dmu.DataMeshAdmin(data_mesh_account_id=MESH_ACCOUNT, region_name=self._region,
                                           log_level=logging.DEBUG, use_creds=self._creds.get(CONSUMER_ADMIN))
        consumer_admin.initialize_consumer_account()
        mesh_admin.enable_account_as_consumer(self._account_ids.get(CONSUMER))


if __name__ == '__main__':
    Step0().setup_central_account()
