import boto3
import pystache
import os
import sys
import json
import shortuuid

sys.path.append(os.path.join(os.path.dirname(__file__), "resource"))

DEFAULT_TAGS = [
    {
        'Key': 'Solution',
        'Value': 'DataMeshUtils'
    },
]
DATA_MESH_MANAGER_ROLENAME = 'DataMeshManager'
DATA_MESH_ADMIN_PRODUCER_ROLENAME = 'DataMeshAdminProducer'
DATA_MESH_ADMIN_CONSUMER_ROLENAME = 'DataMeshAdminConsumer'
DATA_MESH_PRODUCER_ROLENAME = 'DataMeshProducer'
DATA_MESH_CONSUMER_ROLENAME = 'DataMeshConsumer'
DATA_MESH_IAM_PATH = '/AwsDataMesh/'
PRODUCER_S3_POLICY_NAME = 'GetPutS3BucketPolicy'


class DataMeshManager:
    _data_mesh_account_id = None
    _data_producer_account_id = None
    _data_consumer_account_id = None
    _data_mesh_manager_role_arn = None
    _iam_client = None
    _sts_client = None
    _renderer = None

    def __init__(self):
        self._iam_client = boto3.client('iam')
        self._sts_client = boto3.client('sts')
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

    def _create_role_and_attach_policy(self, policy_name: str, policy_desc: str, policy_template: str,
                                       role_name: str, role_desc: str, account_id: str, config: dict = None):
        self._validate_config(config)

        # create an IAM Policy from the template
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
            policy_arn = "arn:aws:iam::%s:policy%s%s" % (account_id, DATA_MESH_IAM_PATH, policy_name)

        role_arn = None
        try:
            # now create the IAM Role
            role_response = self._iam_client.create_role(
                Path=DATA_MESH_IAM_PATH,
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(
                    self._get_assume_role_doc(principal="arn:aws:iam::%s:root" % account_id)),
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
            role_desc='Role to be used for all Data Mesh Management functionality',
            account_id=self._data_mesh_account_id)

    def _create_producer_role(self):
        return self._create_role_and_attach_policy(
            policy_name='DataMeshProducerPolicy',
            policy_desc='IAM Role enabling Accounts to become Data Producers',
            policy_template="producer_policy.pystache",
            role_name=DATA_MESH_ADMIN_PRODUCER_ROLENAME,
            role_desc='Role to be used for all Data Mesh Producer Accounts',
            account_id=self._data_mesh_account_id)

    def _create_consumer_role(self):
        return self._create_role_and_attach_policy(
            policy_name='DataMeshConsumerPolicy',
            policy_desc='IAM Role enabling Accounts to become Data Consumers',
            policy_template="consumer_policy.pystache",
            role_name=DATA_MESH_ADMIN_CONSUMER_ROLENAME,
            role_desc='Role to be used for all Data Mesh Consumer Accounts',
            account_id=self._data_mesh_account_id)

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

    def initialize_producer_account(self, s3_bucket: str):
        '''
        Sets up an AWS Account to act as a Data Provider into the central Data Mesh Account. This method should be invoked
        by an Administrator of the Producer Account. Creates IAM Role & Policy to get and put restricted S3 Bucket Policies.
        Requires at least 1 S3 Bucket Policy be enabled for future grants.
        :return:
        '''
        self._data_producer_account_id = self._sts_client.get_caller_identity().get('Account')
        return self._create_role_and_attach_policy(
            policy_name=PRODUCER_S3_POLICY_NAME,
            policy_desc='IAM Policy enabling Accounts to get and put restricted S3 Bucket Policies',
            policy_template="producer_update_bucket_policy.pystache",
            role_name=DATA_MESH_PRODUCER_ROLENAME,
            role_desc='Role to be used to update S3 Bucket Policies for crawling by Data Mesh Account',
            config={"bucket": s3_bucket},
            account_id=self._data_producer_account_id)

    def _validate_correct_account(self, role_must_exist: str):
        try:
            self._iam_client.get_role(RoleName=role_must_exist)
            return True
        except self._iam_client.exceptions.NoSuchEntityException:
            return False

    def enable_future_bucket_sharing(self, s3_bucket: str):
        '''
        Adds a Bucket and Prefix to the policy document for the DataProducer Role, which will enable the Role to potentially
        share the Bucket with the Data Mesh Account in future. This method does not enable access to the Bucket.
        :param s3_bucket:
        :return:
        '''
        self._data_producer_account_id = self._sts_client.get_caller_identity().get('Account')

        # validate that we are being run within the correct account
        if self._validate_correct_account(DATA_MESH_PRODUCER_ROLENAME) is False:
            raise Exception("Function should be run in the Data Domain Producer Account")

        # get the producer policy
        arn = "arn:aws:iam::%s:policy%s%s" % (
            self._data_producer_account_id, DATA_MESH_IAM_PATH, PRODUCER_S3_POLICY_NAME)
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
            self._iam_client.create_policy_version(PolicyArn=arn, PolicyDocument=json.dumps(policy_doc),
                                                   SetAsDefault=True)

    def grant_data_mesh_account_to_s3_bucket(self, s3_bucket: str, data_mesh_producer_role_arn: str):
        self._data_producer_account_id = self._sts_client.get_caller_identity().get('Account')

        # validate that we are being run within the correct account
        if self._validate_correct_account(DATA_MESH_PRODUCER_ROLENAME) is False:
            raise Exception("Function should be run in the Data Domain Producer Account")

        s3_client = boto3.client('s3')
        get_bucket_policy_response = None
        try:
            get_bucket_policy_response = s3_client.get_bucket_policy(Bucket=s3_bucket,
                                                                     ExpectedBucketOwner=self._data_producer_account_id)
        except s3_client.exceptions.from_code('NoSuchBucketPolicy'):
            pass

        # generate a new statement for the target bucket policy
        statement_sid = "ReadOnly-%s-%s" % (s3_bucket, data_mesh_producer_role_arn)
        conf = {"data_mesh_role_arn": data_mesh_producer_role_arn, "bucket": s3_bucket, "sid": statement_sid}
        statement = json.loads(self._generate_policy(template_file="producer_bucket_policy.pystache", config=conf))

        if get_bucket_policy_response is None or get_bucket_policy_response.get('Policy') is None:
            bucket_policy = {
                "Id": "Policy%s" % shortuuid.uuid(),
                "Version": "2012-10-17",
                "Statement": [
                    statement
                ]
            }

            s3_client.put_bucket_policy(
                Bucket=s3_bucket,
                ConfirmRemoveSelfBucketAccess=False,
                Policy=json.dumps(bucket_policy),
                ExpectedBucketOwner=self._data_producer_account_id
            )
        else:
            bucket_policy = json.loads(get_bucket_policy_response.get('Policy'))
            sid_exists = False
            for s in bucket_policy.get('Statement'):
                if s.get('Sid') == statement_sid:
                    sid_exists = True

            if sid_exists is False:
                # add a statement that allows the data mesh admin producer read-only access
                bucket_policy.get('Statement').append(statement)

                s3_client.put_bucket_policy(
                    Bucket=s3_bucket,
                    ConfirmRemoveSelfBucketAccess=False,
                    Policy=json.dumps(bucket_policy),
                    ExpectedBucketOwner=self._data_producer_account_id
                )

    def initialize_consumer_account(self):
        pass
