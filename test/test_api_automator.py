import unittest
import sys
import os
import warnings
import boto3

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/lib"))

from data_mesh_util.lib.ApiAutomator import *

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

MESH_ACCOUNT = '887210671223'
CONSUMER_ACCOUNT = '206160724517'
PRODUCER_ACCOUNT = '600214582022'


class ApiAutomatorTests(unittest.TestCase):
    _session = boto3.session.Session()
    _automator = ApiAutomator(session=_session, log_level='INFO')
    _proofs = None

    def setUp(self) -> None:
        with open('s3_bucket_policy_proofs.json', 'r') as fp:
            self._proofs = json.load(fp)
            fp.close()

    def test_new_s3_bucket_policy(self):
        new_policy = self._automator._transform_bucket_policy(
            bucket_policy=None, principal_account=PRODUCER_ACCOUNT,
            access_path='s3://org-1-data'
        )
        self.assertEqual(self._proofs.get('New').get('Statement'), new_policy.get('Statement'))

    def test_add_lf_service_role(self):
        # contents of this policy must match the contents of s3_bucket_policy_proofs.json.NoDataMesh
        starting_policy = {
            "Version": "2012-10-17",
            "Id": "Policy1632328705971",
            "Statement": [
                {
                    "Sid": "1234567890",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": [
                            "600214582022"
                        ]
                    },
                    "Action": [
                        "ram:AcceptSharingInvitation"
                    ],
                    "Resource": [
                        "*"
                    ]
                }
            ]}
        new_policy = self._automator._transform_bucket_policy(
            bucket_policy=starting_policy, principal_account=PRODUCER_ACCOUNT,
            access_path='s3://org-1-data'
        )
        self.assertEqual(self._proofs.get('NoDataMesh').get('Statement'), new_policy.get('Statement'))

    def test_add_principal(self):
        # contents of this policy must match the contents of s3_bucket_policy_proofs.json.NewPrincipal
        starting_policy = {
            "Version": "2012-10-17",
            "Id": "Policy1632328705971",
            "Statement": [
                {
                    "Sid": "AwsDataMeshUtilsBucketPolicyStatement-org-1-data",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": [
                            "arn:aws:iam::600214582022:role/aws-service-role/lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess"
                        ]
                    },
                    "Action": [
                        "s3:Get*",
                        "s3:List*"
                    ],
                    "Resource": [
                        "arn:aws:s3:::org-1-data",
                        "arn:aws:s3:::org-1-data/*"
                    ]
                }
            ]}
        new_policy = self._automator._transform_bucket_policy(
            bucket_policy=starting_policy, principal_account=MESH_ACCOUNT,
            access_path='s3://org-1-data'
        )
        self.assertEqual(self._proofs.get('NewPrincipal').get('Statement'), new_policy.get('Statement'))

    def test_no_action(self):
        # contents of this policy must match the contents of s3_bucket_policy_proofs.json.New
        starting_policy = {
            "Version": "2012-10-17",
            "Id": "Policy1632328705971",
            "Statement": [
                {
                    "Sid": "AwsDataMeshUtilsBucketPolicyStatement-org-1-data",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": [
                            "arn:aws:iam::600214582022:role/aws-service-role/lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess"
                        ]
                    },
                    "Action": [
                        "s3:Get*",
                        "s3:List*"
                    ],
                    "Resource": [
                        "arn:aws:s3:::org-1-data",
                        "arn:aws:s3:::org-1-data/*"
                    ]
                }
            ]}
        new_policy = self._automator._transform_bucket_policy(
            bucket_policy=starting_policy, principal_account=PRODUCER_ACCOUNT,
            access_path='s3://org-1-data'
        )
        self.assertEqual(self._proofs.get('New').get('Statement'), new_policy.get('Statement'))
