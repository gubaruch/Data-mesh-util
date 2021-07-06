import boto3
import pystache
import os
import sys
import json

sys.path.append(os.path.join(os.path.dirname(__file__), "resource"))

DEFAULT_TAGS = [
    {
        'Key': 'Solution',
        'Value': 'DataMeshUtils'
    },
]
DATA_MESH_MANAGER_ROLENAME = 'DataMeshManager'
DATA_MESH_IAM_PATH = '/AwsDataMesh/'


class DataMeshManager:
    _data_mesh_account_id = None
    _iam_client = None
    _renderer = None

    def __init__(self):
        self._iam_client = boto3.client('iam')
        sts_client = boto3.client('sts')
        self._data_mesh_account_id = sts_client.get_caller_identity().get('Account')
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
            role_arn = self._iam_client.get_role(RoleName=DATA_MESH_MANAGER_ROLENAME).get('Role').get('Arn')

        # attach the created policy to the role
        self._iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )

        return role_arn

    def _create_data_mesh_manager_role(self):
        return self._create_role_and_attach_policy(policy_name='DataMeshManagerBootstrapPolicy',
                                                   policy_desc='Initial IAM Role enabling the Data Mesh Manager Policy to create future Resource Policies',
                                                   policy_template="data_mesh_setup_iam_policy.pystache",
                                                   role_name=DATA_MESH_MANAGER_ROLENAME,
                                                   role_desc='Role to be used for all Data Mesh Management functionality',
                                                   config={"data_mesh_account_id": self._data_mesh_account_id})

    def initialize_mesh_account(self):
        # create a new IAM role in the Data Mesh Account
        self._data_mesh_account_id = self._create_data_mesh_manager_role()

        return self._data_mesh_account_id

    def initialize_producer_account(self):
        pass
