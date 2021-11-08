try:
    from collections.abc import Mapping  # noqa
except ImportError:
    from collections import Mapping  # noqa

from data_mesh_util.lib.constants import *
import json
import os
import pystache
import botocore
import boto3
import datetime


def make_iam_session_name(current_account):
    val = "%s-%s-%s" % (current_account.get('UserId').replace(":", ""), current_account.get(
        'Account'), datetime.datetime.now().strftime("%Y-%m-%d"))
    n = 64
    if len(val) < n:
        return val
    else:
        return val[:n]


def get_central_role_name(account_id: str, type: str) -> str:
    if type == PRODUCER:
        return f"{DATA_MESH_ADMIN_PRODUCER_ROLENAME}-{account_id}"
    else:
        return f"{DATA_MESH_ADMIN_CONSUMER_ROLENAME}-{account_id}"


def validate_correct_account(credentials, account_id: str, should_match: bool = True):
    caller_account = generate_client(service='sts', region=None, credentials=credentials).get_caller_identity().get(
        'Account')
    if should_match is False and caller_account == account_id:
        raise Exception(
            f"Function should not run within the Data Mesh Account ({account_id}) ")
    if should_match is True and caller_account != account_id:
        raise Exception(
            f"Function should run within the Data Mesh Account ({account_id}) and not {caller_account}")


def convert_s3_path_to_arn(s3_path: str) -> str:
    return f"arn:aws:s3:::{s3_path.replace('s3://', '')}"


def generate_policy(template_file: str, config: dict):
    with open("%s/%s" % (os.path.join(os.path.dirname(__file__), "../resource"), template_file)) as t:
        template = t.read()

    rendered = pystache.Renderer().render(template, config)

    return rendered


def create_assume_role_doc(aws_principals: list = None, resource: str = None, additional_principals: dict = None):
    document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
            }
        ]
    }

    # add the mandatory AWS principals
    if aws_principals is not None:
        document.get('Statement')[0]['Principal'] = {"AWS": aws_principals}

    # add the additional map of principals provided
    if additional_principals is not None:
        for k, v in additional_principals.items():
            document.get('Statement')[0]['Principal'][k] = v

    if resource is not None:
        document.get('Statement')[0]['Resource'] = resource

    return document


def flatten_default_tags():
    output = {}
    for t in DEFAULT_TAGS:
        output[t.get('Key')] = t.get('Value')

    return output


def get_policy_arn(account_id: str, policy_name: str) -> str:
    return "arn:aws:iam::%s:policy%s%s" % (account_id, DATA_MESH_IAM_PATH, policy_name)


def get_role_arn(account_id: str, role_name: str):
    return "arn:aws:iam::%s:role%s%s" % (account_id, DATA_MESH_IAM_PATH, role_name)


def get_producer_role_arn(account_id: str):
    return get_role_arn(account_id, DATA_MESH_PRODUCER_ROLENAME)


def get_consumer_role_arn(account_id: str):
    return get_role_arn(account_id, DATA_MESH_CONSUMER_ROLENAME)


def get_datamesh_producer_role_arn(source_account_id: str, data_mesh_account_id: str):
    return get_role_arn(data_mesh_account_id, get_central_role_name(source_account_id, PRODUCER))


def get_datamesh_consumer_role_arn(source_account_id: str, data_mesh_account_id: str):
    return get_role_arn(data_mesh_account_id, get_central_role_name(source_account_id, CONSUMER))


def _validate_credentials(credentials) -> dict:
    out = {}
    if isinstance(credentials, Mapping):
        out = credentials
    else:
        if credentials is not None:
            # treat as a Boto3 Credentials object
            out = {'AccessKeyId': credentials.access_key, "SecretAccessKey": credentials.secret_key}
            if credentials.token is not None:
                out['SessionToken'] = credentials.token
        else:
            # load from the environment
            out = {'AccessKeyId': os.getenv('AWS_ACCESS_KEY'), "SecretAccessKey": os.getenv('AWS_SECRET_ACCESS_KEY')}
            if credentials.token is not None:
                out['SessionToken'] = os.getenv('AWS_SESSION_TOKEN')

    if out.get('AccessKeyId') is None or out.get('SecretAccessKey') is None:
        raise Exception('Malformed Credentials - missing AccessKeyId or SecretAccessKey')

    return out


def load_ram_shares(lf_client, data_mesh_account_id: str, database_name: str, table_name: str,
                    target_principal: str) -> dict:
    ram_shares = {}

    def _get_ram_share(d: str, t: str = None) -> None:
        share_ref = None
        share_type = None
        # get the permission for the object
        if t is not None:
            resource_ref = {
                'Table': {
                    'CatalogId': data_mesh_account_id,
                    'DatabaseName': d,
                    'Name': t
                }
            }
            share_ref = t
            share_type = 'Table'
        else:
            resource_ref = {
                'Database': {
                    'CatalogId': data_mesh_account_id,
                    'Name': d
                }
            }
            share_ref = d
            share_type = 'Database'

        perm = lf_client.list_permissions(
            CatalogId=data_mesh_account_id,
            ResourceType='TABLE',
            Resource=resource_ref
        )

        if perm is not None:
            for p in perm.get('PrincipalResourcePermissions'):
                if p.get('Principal').get('DataLakePrincipalIdentifier') == target_principal and 'DESCRIBE' in p.get(
                        'Permissions'):
                    ram_shares[share_ref] = {'type': share_type,
                                             'arn': p.get('AdditionalDetails').get('ResourceShare')[0]}
        else:
            raise Exception("Unable to Load RAM Share for Permission")

    # load the RAM shares for the database
    _get_ram_share(database_name)

    # load the RAM shares for the table
    _get_ram_share(database_name, table_name)

    return ram_shares


def create_session(credentials=None, region=None):
    if credentials is not None:
        use_creds = _validate_credentials(credentials)
        args = {
            "aws_access_key_id": use_creds.get('AccessKeyId'),
            "aws_secret_access_key": use_creds.get('SecretAccessKey')
        }
        if region is not None:
            args["region_name"] = region
        else:
            args["region_name"] = os.getenv('AWS_REGION')

        if 'SessionToken' in use_creds:
            args['aws_session_token'] = use_creds.get('SessionToken')

        return boto3.session.Session(**args)
    else:
        return botocore.session.get_session()


def generate_client(service: str, region: str, credentials):
    session = create_session(credentials=credentials, region=region)

    return session.client(service)


def generate_resource(service: str, region: str, credentials):
    use_creds = _validate_credentials(credentials)
    args = {
        "service_name": service,
        "region_name": region,
        "aws_access_key_id": use_creds.get('AccessKeyId'),
        "aws_secret_access_key": use_creds.get('SecretAccessKey')
    }
    if 'SessionToken' in use_creds:
        args['aws_session_token'] = use_creds.get('SessionToken')
    return boto3.resource(**args)
