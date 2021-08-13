import datetime
import time
import boto3
import os
import sys
import json
import shortuuid
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), "resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))

from data_mesh_util.lib.constants import *
import data_mesh_util.lib.utils as utils
from data_mesh_util.lib.SubscriberTracker import SubscriberTracker


class DataMeshConsumer:
    _data_mesh_account_id = None
    _data_consumer_role_arn = None
    _data_consumer_account_id = None
    _data_mesh_manager_role_arn = None
    _data_mesh_sts_session = None
    _iam_client = None
    _sts_client = None
    _config = {}
    _current_region = None
    _log_level = None
    _logger = logging.getLogger("DataMeshConsumer")
    _logger.addHandler(logging.StreamHandler(sys.stdout))
    _subscription_tracker = None

    def __init__(self, data_mesh_account_id: str, log_level: str = "INFO"):
        self._iam_client = boto3.client('iam')
        self._sts_client = boto3.client('sts')
        self._current_region = os.getenv('AWS_REGION')
        self._log_level = log_level
        self._logger.setLevel(log_level)

        # create the subscription tracker
        current_account = self._sts_client.get_caller_identity()
        session_name = utils.make_iam_session_name(current_account)
        self._data_mesh_account_id = data_mesh_account_id
        self._data_consumer_role_arn = utils.get_datamesh_consumer_role_arn(account_id=data_mesh_account_id)
        self._data_mesh_sts_session = self._sts_client.assume_role(RoleArn=self._data_consumer_role_arn,
                                                                   RoleSessionName=session_name)
        self._logger.debug("Created new STS Session for Data Mesh Admin Consumer")
        self._logger.debug(self._data_mesh_sts_session)
        self._subscription_tracker = SubscriberTracker(credentials=self._data_mesh_sts_session.get('Credentials'),
                                                       region_name=self._current_region,
                                                       log_level=self._log_level)

        if self._current_region is None:
            raise Exception("Cannot create a Data Mesh Consumer without AWS_REGION environment variable")

        self._log_level = log_level
        self._logger.setLevel(log_level)

    def _check_acct(self):
        # validate that we are being run within the correct account
        if utils.validate_correct_account(self._iam_client, DATA_MESH_ADMIN_CONSUMER_ROLENAME) is False:
            raise Exception("Function should be run in the Data Consumer Account")

    def request_access_to_product(self, owner_account_id: str, database_name: str,
                                  request_permissions: list, tables: list = None, requesting_principal: str = None):
        '''
        Requests access to a specific data product from the data mesh. Request can be for an entire database, a specific
        table, but is restricted to a single principal. If no principal is provided, grants will be applied to the requesting
        consumer role only. Returns an access request ID which will be approved or denied by the data product owner
        :param database_name:
        :param table_name:
        :param requesting_principal:
        :param request_permissions:
        :return:
        '''
        return self._subscription_tracker.create_subscription_request(
            owner_account_id=owner_account_id,
            database_name=database_name,
            tables=tables,
            principal=requesting_principal,
            request_grants=request_permissions
        )

    def list_product_access(self, principal_id: str):
        '''
        Lists active and pending product access grants.
        :return:
        '''
        pass

    def get_subscription(self, request_id: str):
        return self._subscription_tracker.get_subscription(subscription_id=request_id)

    def delete_subscription(self, subscription_id: str, reason: str):
        pass
