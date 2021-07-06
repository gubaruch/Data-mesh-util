import boto3
import pystache
import os
import sys
import json
import urllib.parse

sys.path.append(os.path.join(os.path.dirname(__file__), "resource"))

DEFAULT_TAGS = [
    {
        'Key': 'Solution',
        'Value': 'DataMeshUtils'
    },
]
DATA_MESH_MANAGER_ROLENAME = 'DataMeshManager'
DATA_MESH_PRODUCER_ROLENAME = 'DataMeshProducer'
DATA_MESH_CONSUMER_ROLENAME = 'DataMeshConsumer'
DATA_MESH_IAM_PATH = '/AwsDataMesh/'
PRODUCER_S3_POLICY_NAME = 'GetPutS3BucketPolicy'


class DataMeshManager:
    _data_mesh_account_id = None
    _data_mesh_manager_role_arn = None
    _iam_client = None
    _sts_client = None
    _renderer = None

    def __init__(self):
        self._iam_client = boto3.client('iam')
        self._sts_client = boto3.client('sts')
        self._data_mesh_account_id = self._sts_client.get_caller_identity().get('Account')
        self._renderer = pystache.Renderer()

    def _generate_policy(self, template_file: str, config: dict):
        with open("%s/%s" % (os.path.join(os.path.dirname(__file__), "resource"), template_file)) as t:
            template = t.read()

        rendered = self._renderer.render(template, config)

        return rendered

    def _get_assume_role_doc(self, principal):
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": principal},
                    "Action": "sts:AssumeRole",
                }
            ]
        }

    def _create_role_and_attach_policy(self, policy_name: str, policy_desc: str, policy_template: str,
                                       role_name: str, role_desc: str, config: dict):
        # add the data mesh account to the config if it isn't provided
        if "data_mesh_account_id" not in config:
            config["data_mesh_account_id"] = self._data_mesh_account_id

        # create an IAM Policy that will allow the role to attach future policies to itself
        policy_doc = self._generate_policy(policy_template, config)

        try:
            response = self._iam_client.create_policy(
                PolicyName=policy_name,
                Path=DATA_MESH_IAM_PATH,
                PolicyDocument=policy_doc,
                Description=policy_desc,
                Tags=DEFAULT_TAGS
            )
            policy_arn = response.get('Policy').get('Arn')
        except self._iam_client.exceptions.EntityAlreadyExistsException:
            policy_arn = "arn:aws:iam::%s:policy%s%s" % (self._data_mesh_account_id, DATA_MESH_IAM_PATH, policy_name)

        role_arn = None
        try:
            # now create the IAM Role
            role_response = self._iam_client.create_role(
                Path=DATA_MESH_IAM_PATH,
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(
                    self._get_assume_role_doc(principal="arn:aws:iam::%s:root" % self._data_mesh_account_id)),
                Description=role_desc,
                Tags=DEFAULT_TAGS
            )

            role_arn = role_response.get('Role').get('Arn')
        except self._iam_client.exceptions.EntityAlreadyExistsException:
            role_arn = self._iam_client.get_role(RoleName=role_name).get(
                'Role').get('Arn')

        # attach the created policy to the role
        self._iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )

        return role_arn

    def _create_data_mesh_manager_role(self):
        self._data_mesh_manager_role_arn = self._create_role_and_attach_policy(
            policy_name='DataMeshManagerBootstrapPolicy',
            policy_desc='Initial IAM Role enabling the Data Mesh Manager Policy to create future Resource Policies',
            policy_template="data_mesh_setup_iam_policy.pystache",
            role_name=DATA_MESH_MANAGER_ROLENAME,
            role_desc='Role to be used for all Data Mesh Management functionality')

    def _create_producer_role(self):
        return self._create_role_and_attach_policy(
            policy_name='DataMeshProducerPolicy',
            policy_desc='IAM Role enabling Accounts to become Data Producers',
            policy_template="producer_policy.pystache",
            role_name=DATA_MESH_PRODUCER_ROLENAME,
            role_desc='Role to be used for all Data Mesh Producer Accounts')

    def _create_consumer_role(self):
        return self._create_role_and_attach_policy(
            policy_name='DataMeshConsumerPolicy',
            policy_desc='IAM Role enabling Accounts to become Data Consumers',
            policy_template="consumer_policy.pystache",
            role_name=DATA_MESH_CONSUMER_ROLENAME,
            role_desc='Role to be used for all Data Mesh Consumer Accounts')

    def initialize_mesh_account(self):
        '''
        Sets up an AWS Account to act as a Data Mesh central account. This method should be invoked by an Administrator
        of the Data Mesh Account. Creates IAM Roles & Policies for the DataMeshManager, DataProducer, and DataConsumer
        :return:
        '''
        # create a new IAM role in the Data Mesh Account to be used for future grants
        self._create_data_mesh_manager_role()

        # create the producer role
        producer_role = self._create_producer_role()

        # create the consumer role
        consumer_role = self._create_consumer_role()

    def initialize_producer_account(self, s3_bucket: str):
        '''
        Sets up an AWS Account to act as a Data Provider into the central Data Mesh Account. This method should be invoked
        by an Administrator of the Producer Account. Creates IAM Role & Policy to get and put restricted S3 Bucket Policies.
        Requires at least 1 S3 Bucket Policy be enabled for future grants.
        :return:
        '''
        return self._create_role_and_attach_policy(
            policy_name=PRODUCER_S3_POLICY_NAME,
            policy_desc='IAM Policy enabling Accounts to get and put restricted S3 Bucket Policies',
            policy_template="producer_update_bucket_policy.pystache",
            role_name=DATA_MESH_PRODUCER_ROLENAME,
            role_desc='Role to be used to update S3 Bucket Policies for crawling by Data Mesh Account',
            config={"bucket": s3_bucket})

    def enable_future_bucket_sharing(self, s3_bucket: str):
        '''
        Adds a Bucket and Prefix to the policy document for the DataProducer Role, which will enable the Role to potentially
        share the Bucket with the Data Mesh Account in future. This method does not enable access to the Bucket.
        :param s3_bucket:
        :return:
        '''
        # get the producer policy
        current_account = self._sts_client.get_caller_identity().get('Account')
        arn = "arn:aws:iam::%s:policy%s%s" % (current_account, DATA_MESH_IAM_PATH, PRODUCER_S3_POLICY_NAME)
        policy_version = self._iam_client.get_policy(PolicyArn=arn).get('Policy').get('DefaultVersionId')
        policy_doc = self._iam_client.get_policy_version(PolicyArn=arn, VersionId=policy_version).get(
            'PolicyVersion').get(
            'Document')

        # update the policy to enable PutBucketPolicy on the bucket
        resources = policy_doc.get('Statement')[0].get('Resource')

        # check that the bucket isn't already in the list
        bucket_arn = "arn:aws:s3:::%s" % s3_bucket
        if bucket_arn not in resources:
            resources.append(bucket_arn)
            policy_doc.get('Statement')[0]['Resource'] = resources
            self._iam_client.create_policy_version(PolicyArn=arn, PolicyDocument=json.dumps(policy_doc), SetAsDefault=True)

    def initialize_consumer_account(self):
        pass
