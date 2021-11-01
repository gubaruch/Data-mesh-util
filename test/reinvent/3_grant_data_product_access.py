import logging
import os
import warnings
import test.test_utils as test_utils
from data_mesh_util.lib.constants import *
import argparse
from data_mesh_util import DataMeshProducer as dmp
from data_mesh_util.lib.SubscriberTracker import *

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)


class Step3():
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

    def grant_access_request(self, subscription_id: str, grant_permissions: list, approval_notes: str):
        # approve access from the producer
        approval = self._mgr.approve_access_request(
            request_id=subscription_id,
            grant_permissions=grant_permissions,
            grantable_permissions=None,
            decision_notes=approval_notes
        )

        return approval


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--subscription_id', dest='subscription_id', required=True)
    parser.add_argument('--grant_permissions', nargs="+", dest='grant_permissions', required=True)
    parser.add_argument('--approval_notes', dest='approval_notes', required=True)

    args = parser.parse_args()
    print(Step3().grant_access_request(subscription_id=args.subscription_id, grant_permissions=args.grant_permissions,
                                       approval_notes=args.approval_notes))
