import boto3
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
import json
import shortuuid
from data_mesh_util.lib.constants import *
import data_mesh_util.lib.utils as utils


class DataMeshAdmin:
    _data_mesh_account_id = None
    _data_producer_account_id = None
    _data_consumer_account_id = None
    _data_mesh_manager_role_arn = None
    _iam_client = None
    _sts_client = None
    _config = {}

    def __init__(self):
        self._iam_client = boto3.client('iam')
        self._sts_client = boto3.client('sts')

    def _validate_config(self, config: dict):
        if config is None:
            config = {}

        # add the data mesh account to the config if it isn't provided
        if "data_mesh_account_id" not in config:
            config["data_mesh_account_id"] = self._data_mesh_account_id

        if "producer_account_id" not in config:
            config["producer_account_id"] = self._data_producer_account_id

        if "consumer_account_id" not in config:
            config["consumer_account_id"] = self._data_consumer_account_id

    def _create_data_mesh_manager_role(self):
        self._validate_config(self._config)

        self._data_mesh_manager_role_arn = utils.create_role_and_attach_policy(
            iam_client=self._iam_client,
            policy_name='DataMeshManagerBootstrapPolicy',
            policy_desc='Initial IAM Role enabling the Data Mesh Manager Policy to create future Resource Policies',
            policy_template="data_mesh_setup_iam_policy.pystache",
            role_name=DATA_MESH_MANAGER_ROLENAME,
            role_desc='Role to be used for all Data Mesh Management functionality',
            account_id=self._data_mesh_account_id,
            config=self._config)

    def _create_producer_role(self):
        self._validate_config(self._config)

        return utils.create_role_and_attach_policy(
            iam_client=self._iam_client,
            policy_name='DataMeshProducerPolicy',
            policy_desc='IAM Role enabling Accounts to become Data Producers',
            policy_template="producer_policy.pystache",
            role_name=DATA_MESH_ADMIN_PRODUCER_ROLENAME,
            role_desc='Role to be used for all Data Mesh Producer Accounts',
            account_id=self._data_mesh_account_id,
            config=self._config)

    def _create_consumer_role(self):
        self._validate_config(self._config)

        return utils.create_role_and_attach_policy(
            iam_client=self._iam_client,
            policy_name='DataMeshConsumerPolicy',
            policy_desc='IAM Role enabling Accounts to become Data Consumers',
            policy_template="consumer_policy.pystache",
            role_name=DATA_MESH_ADMIN_CONSUMER_ROLENAME,
            role_desc='Role to be used for all Data Mesh Consumer Accounts',
            account_id=self._data_mesh_account_id,
            config=self._config)

    def initialize_mesh_account(self):
        '''
        Sets up an AWS Account to act as a Data Mesh central account. This method should be invoked by an Administrator
        of the Data Mesh Account. Creates IAM Roles & Policies for the DataMeshManager, DataProducer, and DataConsumer
        :return:
        '''
        self._data_mesh_account_id = self._sts_client.get_caller_identity().get('Account')

        # create a new IAM role in the Data Mesh Account to be used for future grants
        self._create_data_mesh_manager_role()

        # create the producer role
        producer_role = self._create_producer_role()

        # create the consumer role
        consumer_role = self._create_consumer_role()

        return (self._data_mesh_manager_role_arn, producer_role, consumer_role)
