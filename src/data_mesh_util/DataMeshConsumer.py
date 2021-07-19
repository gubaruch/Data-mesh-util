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


class DataMeshConsumer:
    _data_mesh_account_id = None
    _data_consumer_account_id = None
    _data_consumer_account_id = None
    _data_mesh_manager_role_arn = None
    _iam_client = None
    _sts_client = None
    _config = {}
    _current_region = None
    _logger = logging.getLogger("DataMeshProducer")
    stream_handler = logging.StreamHandler(sys.stdout)
    _logger.addHandler(stream_handler)

    def __init__(self, log_level: str = "INFO"):
        self._iam_client = boto3.client('iam')
        self._sts_client = boto3.client('sts')
        self._current_region = os.getenv('AWS_REGION')

        if self._current_region is None:
            raise Exception("Cannot create a Data Mesh Producer without AWS_REGION environment variable")

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
