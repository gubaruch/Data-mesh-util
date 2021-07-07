import boto3
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
import json
import shortuuid
from data_mesh_util.lib.constants import *
import data_mesh_util.lib.utils as utils

class DataMeshProducer:
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

    def initialize_producer_account(self, s3_bucket: str):
        '''
        Sets up an AWS Account to act as a Data Provider into the central Data Mesh Account. This method should be invoked
        by an Administrator of the Producer Account. Creates IAM Role & Policy to get and put restricted S3 Bucket Policies.
        Requires at least 1 S3 Bucket Policy be enabled for future grants.
        :return:
        '''
        self._data_producer_account_id = self._sts_client.get_caller_identity().get('Account')

        return utils.configure_iam(
            iam_client=self._iam_client,
            policy_name=PRODUCER_S3_POLICY_NAME,
            policy_desc='IAM Policy enabling Accounts to get and put restricted S3 Bucket Policies',
            policy_template="producer_update_bucket_policy.pystache",
            role_name=DATA_MESH_PRODUCER_ROLENAME,
            role_desc='Role to be used to update S3 Bucket Policies for crawling by Data Mesh Account',
            config={"bucket": s3_bucket},
            account_id=self._data_producer_account_id)

    def enable_future_bucket_sharing(self, s3_bucket: str):
        '''
        Adds a Bucket and Prefix to the policy document for the DataProducer Role, which will enable the Role to potentially
        share the Bucket with the Data Mesh Account in future. This method does not enable access to the Bucket.
        :param s3_bucket:
        :return:
        '''
        self._data_producer_account_id = self._sts_client.get_caller_identity().get('Account')

        # validate that we are being run within the correct account
        if utils.validate_correct_account(self._iam_client, DATA_MESH_PRODUCER_ROLENAME) is False:
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
        if utils.validate_correct_account(self._iam_client, DATA_MESH_PRODUCER_ROLENAME) is False:
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
        statement = json.loads(utils.generate_policy(template_file="producer_bucket_policy.pystache", config=conf))

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
