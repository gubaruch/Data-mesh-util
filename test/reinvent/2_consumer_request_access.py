import argparse
import warnings
import sys
import os
import inspect
two_up = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))))
sys.path.insert(0, two_up)

import test.test_utils as test_utils
from data_mesh_util.lib.constants import *
from data_mesh_util import DataMeshConsumer as dmc
import data_mesh_util.lib.utils as utils
import logging

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)


class Step2():
    '''
    Consumer functionality to create a subscription request.
    '''
    _region, _clients, _account_ids, _creds = test_utils.load_client_info_from_file()

    # bind the test class into the consumer account
    _sts_session = test_utils.assume_source_role(sts_client=_clients.get(CONSUMER),
                                                 account_id=_account_ids.get(CONSUMER),
                                                 type=CONSUMER)
    consumer_credentials = _sts_session.get('Credentials')
    _sts_client = utils.generate_client('sts', _region, consumer_credentials)

    _mgr = dmc.DataMeshConsumer(data_mesh_account_id=_account_ids.get(MESH),
                                log_level=logging.DEBUG,
                                region_name=_region,
                                use_credentials=consumer_credentials)

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def create_subscription_request(self, database_name: str, tables: list, request_permissions: list):
        sub = self._mgr.request_access_to_product(
            owner_account_id=self._account_ids.get(PRODUCER),
            database_name=database_name,
            tables=tables,
            request_permissions=request_permissions
        )
        return sub.get("SubscriptionId")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--database_name', dest='database_name', required=True)
    parser.add_argument('--tables', nargs="+", dest='tables', required=True)
    parser.add_argument('--request_permissions', nargs="+", dest='request_permissions', required=True)

    args = parser.parse_args()
    print(Step2().create_subscription_request(database_name=args.database_name, tables=args.tables,
                                              request_permissions=args.request_permissions))
