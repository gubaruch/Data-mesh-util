import argparse
import warnings
import sys
import os
import inspect
two_up = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))))
sys.path.insert(0, two_up)

import test.test_utils as test_utils
from data_mesh_util.lib.constants import *
from data_mesh_util import DataMeshProducer as dmp
from data_mesh_util.lib.SubscriberTracker import *
import logging

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)


class Step1():
    '''
    Create a data product. Should be run using credentials for a principal who can assume
    the DataMeshAdminProducer role in the data mesh.
    '''
    _region, _clients, _account_ids, _creds = test_utils.load_client_info_from_file()

    # bind the test class into the producer account
    _sts_session = test_utils.assume_source_role(sts_client=_clients.get(PRODUCER),
                                                 account_id=_account_ids.get(PRODUCER),
                                                 type=PRODUCER)
    producer_credentials = _sts_session.get('Credentials')
    _sts_client = utils.generate_client('sts', _region, producer_credentials)

    _mgr = dmp.DataMeshProducer(data_mesh_account_id=_account_ids.get(MESH),
                                log_level=logging.DEBUG,
                                region_name=_region,
                                use_credentials=producer_credentials)
    _subscription_tracker = SubscriberTracker(data_mesh_account_id=_account_ids.get(MESH),
                                              credentials=_creds.get(MESH),
                                              region_name=_region,
                                              log_level=logging.DEBUG)

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def create_data_product(self, database_name: str, table_regex: str, cron_expr: str, crawler_role: str):
        self._mgr.create_data_products(
            source_database_name=database_name,
            table_name_regex=table_regex,
            create_public_metadata=True,
            sync_mesh_catalog_schedule=cron_expr,
            sync_mesh_crawler_role_arn=crawler_role
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--database_name', dest='database_name', required=True)
    parser.add_argument('--table_regex', dest='table_regex', required=True)
    parser.add_argument('--cron_expr', dest='cron_expr', required=False)
    parser.add_argument('--crawler_role', dest='crawler_role', required=False)

    args = parser.parse_args()
    Step1().create_data_product(args.database_name, args.table_regex, args.cron_expr, args.crawler_role)
