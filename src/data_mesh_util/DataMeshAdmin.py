import boto3
import os
import sys
import logging
import time

import botocore.session

sys.path.append(os.path.join(os.path.dirname(__file__), "resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
from data_mesh_util.lib.constants import *
import data_mesh_util.lib.utils as utils
from data_mesh_util.lib.SubscriberTracker import SubscriberTracker


class DataMeshAdmin:
    _region = None
    _current_credentials = None
    _data_mesh_account_id = None
    _data_producer_account_id = None
    _data_producer_role_arn = None
    _data_consumer_account_id = None
    _data_consumer_role_arn = None
    _data_mesh_manager_role_arn = None
    _session = None
    _iam_client = None
    _lf_client = None
    _sts_client = None
    _dynamo_client = None
    _dynamo_resource = None
    _config = {}
    _logger = logging.getLogger("DataMeshAdmin")
    _log_level = None
    stream_handler = logging.StreamHandler(sys.stdout)
    _logger.addHandler(stream_handler)
    _subscriber_tracker = None

    def __init__(self, data_mesh_account_id: str, region_name: str = 'us-east-1', log_level: str = "INFO",
                 use_creds=None):
        self._data_mesh_account_id = data_mesh_account_id
        # get the region for the module
        if 'AWS_REGION' in os.environ:
            self._region = os.environ.get('AWS_REGION')
        else:
            if region_name is None:
                raise Exception("Cannot initialize a Data Mesh without an AWS Region")
            else:
                self._region = region_name

        if use_creds is None:
            self._session = botocore.session.get_session()
            self._iam_client = boto3.client('iam', region_name=self._region)
            self._sts_client = boto3.client('sts', region_name=self._region)
            self._dynamo_client = boto3.client('dynamodb', region_name=self._region)
            self._dynamo_resource = boto3.resource('dynamodb', region_name=self._region)
            self._lf_client = boto3.client('lakeformation', region_name=self._region)
        else:
            self._session = utils.create_session(credentials=use_creds, region=self._region)
            self._iam_client = self._session.client('iam')
            self._sts_client = self._session.client('sts')
            self._dynamo_client = self._session.client('dynamodb')
            self._dynamo_resource = self._session.client('dynamodb')
            self._lf_client = self._session.client('lakeformation')

        self._logger.setLevel(log_level)
        self._log_level = log_level

    def _create_template_config(self, config: dict):
        if config is None:
            config = {}

        # add the data mesh account to the config if it isn't provided
        if "data_mesh_account_id" not in config:
            config["data_mesh_account_id"] = self._data_mesh_account_id

        if "producer_account_id" not in config:
            config["producer_account_id"] = self._data_producer_account_id

        if "consumer_account_id" not in config:
            config["consumer_account_id"] = self._data_consumer_account_id

        self._logger.debug(self._config)

    def _create_data_mesh_manager_role(self):
        '''
        Private method to create objects needed for an administrative role that can be used to grant access to Data Mesh roles
        :return:
        '''
        utils.validate_correct_account(credentials=botocore.session.get_session().get_credentials(),
                                       account_id=self._data_mesh_account_id)

        self._create_template_config(self._config)

        current_identity = self._sts_client.get_caller_identity()
        self._logger.debug("Running as %s" % str(current_identity))

        mgr_tuple = utils.configure_iam(
            iam_client=self._iam_client,
            policy_name='DataMeshManagerPolicy',
            policy_desc='IAM Policy to bootstrap the Data Mesh Admin',
            policy_template="data_mesh_setup_iam_policy.pystache",
            role_name=DATA_MESH_MANAGER_ROLENAME,
            role_desc='Role to be used for the Data Mesh Manager function',
            account_id=self._data_mesh_account_id,
            config=self._config,
            logger=self._logger)
        data_mesh_mgr_role_arn = mgr_tuple[0]

        self._logger.info("Validated Data Mesh Manager Role %s" % data_mesh_mgr_role_arn)

        # Horrible retry logic required to avoid boto3 exception using a role as a principal too soon after it's been created
        retries = 0
        while True:
            try:
                # remove default IAM settings in lakeformation for the account, and setup the manager role and this caller as admins
                response = self._lf_client.put_data_lake_settings(
                    DataLakeSettings={
                        "DataLakeAdmins": [
                            {"DataLakePrincipalIdentifier": data_mesh_mgr_role_arn},
                            # add the current caller identity as an admin
                            {"DataLakePrincipalIdentifier": current_identity.get('Arn')}
                        ],
                        'CreateTableDefaultPermissions': []
                    }
                )
            except self._lf_client.exceptions.InvalidInputException:
                self._logger.info(f"Error setting DataLake Principal as {data_mesh_mgr_role_arn}. Backing off....")
                retries += 1
                if retries > 5:
                    raise
                time.sleep(3)
                continue
            break
        self._logger.info(
            "Removed default data lake settings for Account %s. New Admins are %s and Data Mesh Manager" % (
                current_identity.get('Account'), current_identity.get('Arn')))

        return mgr_tuple

    def _create_producer_role(self):
        '''
        Private method to create objects needed for a Producer account to connect to the Data Mesh and create data products
        :return:
        '''
        self._create_template_config(self._config)

        # create the policy and role to be used for data producers
        producer_tuple = utils.configure_iam(
            iam_client=self._iam_client,
            policy_name='DataMeshProducerPolicy',
            policy_desc='IAM Role enabling Accounts to become Data Producers',
            policy_template="producer_policy.pystache",
            role_name=DATA_MESH_ADMIN_PRODUCER_ROLENAME,
            role_desc='Role to be used for all Data Mesh Producer Accounts',
            account_id=self._data_mesh_account_id,
            config=self._config,
            logger=self._logger)
        producer_iam_role_arn = producer_tuple[0]

        self._logger.info("Validated Data Mesh Producer Role %s" % producer_iam_role_arn)

        # Horrible retry logic required to avoid boto3 exception using a role as a principal too soon after it's been created
        retries = 0
        while True:
            try:
                # grant this role the ability to create databases and tables
                response = self._lf_client.grant_permissions(
                    Principal={
                        'DataLakePrincipalIdentifier': producer_iam_role_arn
                    },
                    Resource={'Catalog': {}},
                    Permissions=[
                        'CREATE_DATABASE'
                    ]
                )
            except self._lf_client.exceptions.InvalidInputException:
                self._logger.info(f"Error granting CREATE_DATABASE to {producer_iam_role_arn}. Backing off....")
                retries += 1
                if retries > 5:
                    raise
                time.sleep(3)
                continue
            break
        self._logger.info("Granted Data Mesh Producer CREATE_DATABASE privileges on Catalog")

        return producer_tuple

    def _create_consumer_role(self):
        '''
        Private method to create objects needed for a Consumer account to connect to the Data Mesh and mirror data
        products into their account
        :return:
        '''
        self._create_template_config(self._config)

        return utils.configure_iam(
            iam_client=self._iam_client,
            policy_name='DataMeshConsumerPolicy',
            policy_desc='IAM Role enabling Accounts to become Data Consumers',
            policy_template="consumer_policy.pystache",
            role_name=DATA_MESH_ADMIN_CONSUMER_ROLENAME,
            role_desc='Role to be used for all Data Mesh Consumer Accounts',
            account_id=self._data_mesh_account_id,
            config=self._config,
            logger=self._logger)

    def _api_tuple(self, item_tuple: tuple):
        return {
            "RoleArn": item_tuple[0],
            "UserArn": item_tuple[1],
            "GroupArn": item_tuple[2]
        }

    def initialize_mesh_account(self):
        '''
        Sets up an AWS Account to act as a Data Mesh central account. This method should be invoked by an Administrator
        of the Data Mesh Account. Creates IAM Roles & Policies for the DataMeshManager, DataProducer, and DataConsumer
        :return:
        '''
        self._data_mesh_account_id = self._sts_client.get_caller_identity().get('Account')

        self._current_credentials = boto3.session.Session().get_credentials()
        self._subscription_tracker = SubscriberTracker(data_mesh_account_id=self._data_mesh_account_id,
                                                       credentials=self._current_credentials,
                                                       region_name=self._region,
                                                       log_level=self._log_level)

        # create a new IAM role in the Data Mesh Account to be used for future grants
        mgr_tuple = self._create_data_mesh_manager_role()

        # create the producer role
        producer_tuple = self._create_producer_role()

        # create the consumer role
        consumer_tuple = self._create_consumer_role()

        return {
            "Manager": self._api_tuple(mgr_tuple),
            "ProducerAdmin": self._api_tuple(producer_tuple),
            "ConsumerAdmin": self._api_tuple(consumer_tuple),
            "SubscriptionTracker": self._subscription_tracker.get_endpoints()
        }

    # TODO move method to CloudFormation based provisioning
    def initialize_producer_account(self):
        '''
        Sets up an AWS Account to act as a Data Provider into the central Data Mesh Account. This method should be invoked
        by an Administrator of the Producer Account. Creates IAM Role & Policy to get and put restricted S3 Bucket Policies.
        Requires at least 1 S3 Bucket Policy be enabled for future grants.
        :return:
        '''
        return self._initialize_account_as(type=PRODUCER)

    def enable_account_as_producer(self, account_id: str):
        '''
        Enables a remote account to act as a data producer by granting them access to the DataMeshAdminProducer Role
        :return:
        '''
        utils.validate_correct_account(self._session.get_credentials(), self._data_mesh_account_id)

        # create trust relationships for the AdminProducer roles
        utils.add_aws_trust_to_role(iam_client=self._iam_client, account_id=account_id,
                                    role_name=DATA_MESH_ADMIN_PRODUCER_ROLENAME)
        self._logger.info("Enabled Account %s to assume %s" % (account_id, DATA_MESH_ADMIN_PRODUCER_ROLENAME))

    def _initialize_account_as(self, type: str):
        '''
        Sets up an AWS Account to act as a Data Consumer from the central Data Mesh Account. This method should be invoked
        by an Administrator of the Consumer Account. Creates IAM Role & Policy which allows an end user to assume the
        DataMeshAdminConsumer Role and subscribe to products.
        :return:
        '''
        utils.validate_correct_account(self._session.get_credentials(), self._data_mesh_account_id,
                                       should_match=False)

        source_account = self._sts_client.get_caller_identity().get('Account')

        local_role_name = None
        remote_role_name = None
        policy_name = None
        policy_template = None
        if type == CONSUMER:
            self._data_consumer_account_id = source_account
            local_role_name = DATA_MESH_CONSUMER_ROLENAME
            remote_role_name = DATA_MESH_ADMIN_CONSUMER_ROLENAME
            policy_name = CONSUMER_POLICY_NAME
            policy_template = "consumer_policy.pystache"
            target_account = self._data_consumer_account_id
        else:
            self._data_producer_account_id = source_account
            local_role_name = DATA_MESH_PRODUCER_ROLENAME
            remote_role_name = DATA_MESH_ADMIN_PRODUCER_ROLENAME
            policy_name = PRODUCER_POLICY_NAME
            policy_template = "producer_access_catalog.pystache"
            target_account = self._data_producer_account_id

        self._logger.info(f"Setting up Account {source_account} as a Data {type}")

        group_name = f"{local_role_name}Group"

        # setup the consumer IAM role, user, and group
        iam_details = utils.configure_iam(
            iam_client=self._iam_client,
            policy_name=policy_name,
            policy_desc=f'IAM Policy enabling Accounts to Assume the {local_role_name} Role',
            policy_template=policy_template,
            role_name=local_role_name,
            role_desc=f'{local_role_name} facilitating principals to act as {type}',
            account_id=target_account,
            logger=self._logger
        )

        self._logger.info(f"Role {iam_details[0]}")
        self._logger.info(f"User {iam_details[1]}")
        self._logger.info(f"Group {iam_details[2]}")

        remote_role_arn = None
        if type == CONSUMER:
            self._data_consumer_role_arn = iam_details[0]
            remote_role_arn = utils.get_datamesh_consumer_role_arn(account_id=self._data_mesh_account_id)
        else:
            self._data_producer_role_arn = iam_details[0]
            remote_role_arn = utils.get_datamesh_producer_role_arn(account_id=self._data_mesh_account_id)

        # allow the local group to assume the remote consumer policy
        policy_name = f"Assume{remote_role_name}"

        policy_arn = utils.create_assume_role_policy(
            iam_client=self._iam_client,
            account_id=target_account,
            policy_name=policy_name,
            role_arn=remote_role_arn,
            logger=self._logger
        )
        self._logger.info(f"Validated Policy {policy_name} as {policy_arn}")
        self._iam_client.attach_group_policy(GroupName=group_name, PolicyArn=policy_arn)
        self._logger.info(f"Bound {policy_arn} to Group {group_name}")

        # make the iam role a lakeformation admin
        utils.add_datalake_admin(lf_client=self._lf_client, principal=iam_details[0])

        return iam_details

    def initialize_consumer_account(self):
        '''
        Sets up an AWS Account to act as a Data Consumer from the central Data Mesh Account. This method should be invoked
        by an Administrator of the Consumer Account. Creates IAM Role & Policy which allows an end user to assume the
        DataMeshAdminConsumer Role and subscribe to products.
        :return:
        '''
        return self._initialize_account_as(type=CONSUMER)

    def enable_account_as_consumer(self, account_id: str):
        '''
        Enables a remote account to act as a data consumer by granting them access to the DataMeshAdminConsumer Role
        :return:
        '''
        utils.validate_correct_account(self._session.get_credentials(), self._data_mesh_account_id)

        # create trust relationships for the AdminProducer roles
        utils.add_aws_trust_to_role(iam_client=self._iam_client, account_id=account_id,
                                    role_name=DATA_MESH_ADMIN_CONSUMER_ROLENAME)
        self._logger.info("Enabled Account %s to assume %s" % (account_id, DATA_MESH_ADMIN_CONSUMER_ROLENAME))
