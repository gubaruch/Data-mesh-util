import sys
import logging

import botocore.exceptions
import shortuuid

from data_mesh_util.lib.constants import *
import json
import data_mesh_util.lib.utils as utils


class ApiAutomator:
    _session = None
    _logger = None
    _region = None
    _logger = logging.getLogger("ApiAutomator")
    # make sure we always log to standard out
    _logger.addHandler(logging.StreamHandler(sys.stdout))
    _clients = {}

    def __init__(self, session, log_level: str = "INFO"):
        self._session = session
        self._logger.setLevel(log_level)

    def _get_client(self, client_name):
        client = self._clients.get(client_name)

        if client is None:
            client = self._session.client(client_name)
            self._clients[client_name] = client

        return client

    def _get_bucket_name(self, bucket_value):
        if 's3://' in bucket_value:
            return bucket_value.split('/')[2]
        else:
            return bucket_value

    def _transform_bucket_policy(self, bucket_policy: dict, principal_account: str,
                                 access_path: str) -> dict:
        use_bucket_name = self._get_bucket_name(access_path)
        policy_sid = f"{BUCKET_POLICY_STATEMENT_SID}-{use_bucket_name}"

        # generate a new bucket policy from the template
        s3_path = "/".join(access_path.split("/")[2:])
        base_policy = json.loads(utils.generate_policy(template_file='producer_bucket_policy.pystache', config={
            'account_id': principal_account,
            'access_path': s3_path,
            'sid': policy_sid
        }))

        if bucket_policy is None:
            generated_policy = {
                "Version": "2012-10-17",
                "Id": shortuuid.uuid(),
                "Statement": [
                    base_policy
                ]
            }
            self._logger.info(
                f"Creation new S3 Bucket policy enabling Data Mesh LakeFormation Service Role access for {principal_account}")
            self._logger.info(f"Creating new Bucket Policy for {access_path}")
            return generated_policy
        else:
            # we already have a bucket policy, so determine if there is already a data mesh grant created for this bucket
            statements = bucket_policy.get('Statement')
            data_mesh_statement_index = -1

            for i, s in enumerate(statements):
                if s.get('Sid') == policy_sid:
                    data_mesh_statement_index = i
                    break

            if data_mesh_statement_index == -1:
                # there was not a previously created data mesh auth statement, so add it to the end
                statements.append(base_policy)
                self._logger.info(
                    f"Adding new Data Mesh LakeFormation Service Role statement for {principal_account} to existing Bucket Policy")
            else:
                # we already have a data mesh auth statement, so check if the principal is already there first
                statement = statements[data_mesh_statement_index]
                set_principal = f"arn:aws:iam::{principal_account}:role/aws-service-role/lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess"
                if set_principal not in statement.get('Principal').get('AWS'):
                    current_principals = statement.get('Principal').get('AWS')
                    if isinstance(current_principals, list):
                        statement.get('Principal').get('AWS').append(set_principal)
                    else:
                        statement.get('Principal')['AWS'] = [current_principals, set_principal]

                    statements[data_mesh_statement_index] = statement
                    self._logger.info(
                        f"Adding principal {principal_account} to existing Data Mesh LakeFormation Service Role statement")

                    bucket_policy['Statement'] = statements
                else:
                    self._logger.info("Not modifying bucket policy as principal has already been added")

            return bucket_policy

    def _get_current_bucket_policy(self, s3_client, bucket_name: str):
        try:
            current_policy = s3_client.get_bucket_policy(Bucket=bucket_name)
            return current_policy
        except botocore.exceptions.ClientError as ce:
            if 'NoSuchBucketPolicy' in str(ce):
                return None
            else:
                raise ce

    def add_bucket_policy_entry(self, principal_account: str, access_path: str):
        s3_client = self._get_client('s3')

        bucket_name = self._get_bucket_name(access_path)

        # get the existing policy, if there is one
        current_policy = self._get_current_bucket_policy(s3_client, bucket_name)

        bucket_policy = None
        if current_policy is not None:
            bucket_policy = json.loads(current_policy.get('Policy'))

        # transform the existing or None policy into the desired target lakeformation policy
        new_policy = self._transform_bucket_policy(
            bucket_policy=bucket_policy, principal_account=principal_account,
            access_path=access_path
        )

        # put the policy back into the bucket store
        s3_client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(new_policy))
