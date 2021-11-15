import datetime
import time
import boto3
import os
import sys
import json

import botocore.session
import shortuuid
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), "resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))

from data_mesh_util.lib.ApiAutomator import ApiAutomator
from data_mesh_util.lib.SubscriberTracker import *


class DataMeshConsumer:
    _current_account = None
    _data_mesh_account_id = None
    _data_consumer_role_arn = None
    _data_consumer_account_id = None
    _data_mesh_manager_role_arn = None
    _data_mesh_sts_session = None
    _session = None
    _iam_client = None
    _lf_client = None
    _ram_client = None
    _lf_client = None
    _sts_client = None
    _config = {}
    _current_region = None
    _log_level = None
    _logger = logging.getLogger("DataMeshConsumer")
    _logger.addHandler(logging.StreamHandler(sys.stdout))
    _subscription_tracker = None
    _consumer_automator = None

    def __init__(self, data_mesh_account_id: str, region_name: str, log_level: str = "INFO", use_credentials=None):
        if region_name is None:
            raise Exception("Cannot initialize a Data Mesh Consumer without an AWS Region")
        else:
            self._current_region = region_name

        # Assume the consumer account DataMeshConsumer role, unless we have been supplied temporary credentials for that role
        # bind the test class into the producer account
        _sts_client = utils.generate_client('sts', region_name, use_credentials)
        _current_identity = _sts_client.get_caller_identity()
        set_credentials = None
        if _current_identity.get('Arn') == utils.get_role_arn(account_id=_current_identity.get('Account'),
                                                              role_name=DATA_MESH_CONSUMER_ROLENAME):
            set_credentials = use_credentials
        else:
            _sts_session = _sts_client.assume_role(
                RoleArn=utils.get_role_arn(_current_identity.get('Account'), DATA_MESH_CONSUMER_ROLENAME),
                RoleSessionName=utils.make_iam_session_name(_current_identity)
            )

            set_credentials = _sts_session.get('Credentials')

        if use_credentials is None:
            self._session = boto3.session.Session(region_name=self._current_region)
        else:
            self._session = utils.create_session(credentials=set_credentials, region=self._current_region)

        self._iam_client = self._session.client('iam')
        self._ram_client = self._session.client('ram')
        self._sts_client = self._session.client('sts')
        self._glue_client = self._session.client('glue')

        self._log_level = log_level
        self._logger.setLevel(log_level)

        self._current_account = self._session.client('sts').get_caller_identity()
        self._data_consumer_account_id = self._current_account.get('Account')

        self._consumer_automator = ApiAutomator(target_account=self._data_consumer_account_id,
                                                session=self._session, log_level=self._log_level)

        # assume the consumer role in the mesh
        session_name = utils.make_iam_session_name(self._current_account)
        self._data_mesh_account_id = data_mesh_account_id
        self._data_consumer_role_arn = utils.get_datamesh_consumer_role_arn(
            source_account_id=self._current_account.get('Account'),
            data_mesh_account_id=data_mesh_account_id
        )
        self._data_mesh_sts_session = self._sts_client.assume_role(RoleArn=self._data_consumer_role_arn,
                                                                   RoleSessionName=session_name)
        self._logger.debug("Created new STS Session for Data Mesh Admin Consumer")
        self._logger.debug(self._data_mesh_sts_session)

        utils.validate_correct_account(self._data_mesh_sts_session.get('Credentials'), data_mesh_account_id)

        # create the subscription tracker
        self._subscription_tracker = SubscriberTracker(credentials=self._data_mesh_sts_session.get('Credentials'),
                                                       data_mesh_account_id=data_mesh_account_id,
                                                       region_name=self._current_region,
                                                       log_level=self._log_level)

        self._log_level = log_level
        self._logger.setLevel(log_level)

    def request_access_to_product(self, owner_account_id: str, database_name: str,
                                  request_permissions: list, tables: list = None) -> dict:
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
            principal=self._current_account.get('Account'),
            request_grants=request_permissions,
            suppress_object_validation=True
        )

    def finalize_subscription(self, subscription_id: str) -> None:
        '''
        Finalizes the process of requesting access to a data product. This imports the granted subscription into the consumer's account
        :param subscription_id:
        :return:
        '''
        # grab the subscription
        subscription = self._subscription_tracker.get_subscription(subscription_id=subscription_id)
        data_mesh_database_name = subscription.get(DATABASE_NAME)

        # create a shared database reference
        self._consumer_automator.get_or_create_database(
            database_name=data_mesh_database_name,
            database_desc=f"Database to contain objects from Producer Database {subscription.get(OWNER_PRINCIPAL)}.{subscription.get(DATABASE_NAME)}",
            source_account=self._data_mesh_account_id
        )

        self._consumer_automator.accept_pending_lf_resource_shares(
            sender_account=self._data_mesh_account_id
        )

    def get_subscription(self, request_id: str) -> dict:
        return self._subscription_tracker.get_subscription(subscription_id=request_id)

    def list_product_access(self) -> dict:
        '''
        Lists active and pending product access grants.
        :return:
        '''
        me = self._sts_client.get_caller_identity().get('Account')
        return self._subscription_tracker.list_subscriptions(owner_id=me)

    def delete_subscription(self, subscription_id: str, reason: str):
        '''
        Soft delete a subscription
        :param subscription_id:
        :param reason:
        :return:
        '''
        subscription = self._subscription_tracker.get_subscription(subscription_id=subscription_id)

        # confirm that we are calling from the same account as the subscriber principal
        if subscription.get(SUBSCRIBER_PRINCIPAL) != self._current_account.get('Account'):
            raise Exception("Cannot delete permissions which you do not own")
        else:
            # leave the ram shares
            self._consumer_automator.leave_ram_shares(principal=subscription.get(SUBSCRIBER_PRINCIPAL),
                                                      ram_shares=subscription.get(RAM_SHARES))

            return self._subscription_tracker.delete_subscription(subscription_id=subscription_id, reason=reason)
