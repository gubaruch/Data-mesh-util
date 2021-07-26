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
    _data_consumer_account_id = None
    _data_consumer_account_id = None
    _data_mesh_manager_role_arn = None
    _iam_client = None
    _sts_client = None
    _config = {}
    _current_region = None
    _log_level = None
    _logger = logging.getLogger("DataMeshConsumer")
    stream_handler = logging.StreamHandler(sys.stdout)
    _logger.addHandler(stream_handler)

    def __init__(self, log_level: str = "INFO"):
        self._iam_client = boto3.client('iam')
        self._sts_client = boto3.client('sts')
        self._current_region = os.getenv('AWS_REGION')

        if self._current_region is None:
            raise Exception("Cannot create a Data Mesh Consumer without AWS_REGION environment variable")

        self._log_level = log_level
        self._logger.setLevel(log_level)

    def _check_acct(self):
        # validate that we are being run within the correct account
        if utils.validate_correct_account(self._iam_client, DATA_MESH_ADMIN_CONSUMER_ROLENAME) is False:
            raise Exception("Function should be run in the Data Consumer Account")

    def initialize_consumer_account(self, data_mesh_account_id: str):
        '''
        Sets up an AWS Account to act as a Data Consumer from the central Data Mesh Account. This method should be invoked
        by an Administrator of the Consumer Account. Creates IAM Role & Policy which allows an end user to assume the
        DataMeshAdminConsumer Role and subscribe to products.
        :return:
        '''
        self._check_acct()

        self._data_consumer_account_id = self._sts_client.get_caller_identity().get('Account')
        self._logger.info("Setting up Account %s as a Data Consumer" % self._data_consumer_account_id)

        # setup the consumer IAM role
        consumer_iam = utils.configure_iam(
            iam_client=self._iam_client,
            policy_name=CONSUMER_POLICY_NAME,
            policy_desc='IAM Policy enabling Accounts to Assume the DataMeshAdminConsumer Role',
            policy_template="consumer_policy.pystache",
            role_name=DATA_MESH_CONSUMER_ROLENAME,
            role_desc='Role to be used to update S3 Bucket Policies for access by the Data Mesh Account',
            account_id=self._data_consumer_account_id)

        # now create the iam policy allowing the producer group to assume the data mesh producer role
        datamesh_admin_consumer_role_arn = utils.get_datamesh_consumer_role_arn(account_id=data_mesh_account_id)

        policy_name = "AssumeDataMeshAdminConsumer"
        policy_arn = utils.create_assume_role_policy(
            iam_client=self._iam_client,
            account_id=self._data_consumer_account_id,
            policy_name=policy_name,
            role_arn=datamesh_admin_consumer_role_arn
        )
        self._logger.info("Created new IAM Policy %s" % policy_arn)

        # now let the group assume the cross account role
        group_name = "%sGroup" % DATA_MESH_CONSUMER_ROLENAME
        self._iam_client.attach_group_policy(GroupName=group_name, PolicyArn=policy_arn)
        self._logger.info("Attached Policy to Group %s" % group_name)

        return consumer_iam

    def request_access_to_product(self, data_mesh_account_id: str, owner_account_id: str, database_name: str,
                                  request_permissions: list, table_name: str = None, requesting_principal: str = None):
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
        # assume the consumer role in the data mesh account
        data_mesh_consumer_role_arn = utils.get_datamesh_consumer_role_arn(account_id=data_mesh_account_id)
        current_account = self._sts_client.get_caller_identity()
        session_name = "%s-%s-%s" % (current_account.get('UserId'), current_account.get(
            'Account'), datetime.datetime.now().strftime("%Y-%m-%d"))
        data_mesh_sts_session = self._sts_client.assume_role(RoleArn=data_mesh_consumer_role_arn,
                                                             RoleSessionName=session_name)
        self._logger.debug("Created new STS Session for Data Mesh Admin Producer")
        self._logger.debug(data_mesh_sts_session)

        # create a subscriber tracker in the data mesh consumer session
        ddb_client = utils.generate_client(service='dynamodb', region=self._current_region,
                                           credentials=data_mesh_sts_session.get('Credentials'))
        ddb_resource = utils.generate_resource(service='dynamodb', region=self._current_region,
                                               credentials=data_mesh_sts_session.get('Credentials'))
        subscription_tracker = SubscriberTracker(dynamo_client=ddb_client, dynamo_resource=ddb_resource,
                                                 log_level=self._log_level)

        return subscription_tracker.create_subscription_request(
            owner_account_id=owner_account_id,
            database_name=database_name,
            table_name=table_name,
            principal=requesting_principal,
            request_grants=request_permissions
        )

    def list_product_access(self):
        '''
        Lists active and pending product access grants.
        :return:
        '''
        pass
