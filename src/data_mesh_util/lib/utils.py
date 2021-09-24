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


def validate_correct_account(credentials, account_id: str, should_match: bool = True):
    caller_account = generate_client(service='sts', region=None, credentials=credentials).get_caller_identity().get(
        'Account')
    if should_match is False and caller_account == account_id:
        raise Exception(
            f"Function should run within the Data Mesh Account ({account_id}) and not {caller_account}")
    if should_match is True and caller_account != account_id:
        raise Exception(
            f"Function should not run within the Data Mesh Account ({account_id}) ")


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


def get_role_arn(account_id: str, role_name: str):
    return "arn:aws:iam::%s:role%s%s" % (account_id, DATA_MESH_IAM_PATH, role_name)


def get_producer_role_arn(account_id: str):
    return get_role_arn(account_id, DATA_MESH_PRODUCER_ROLENAME)


def get_consumer_role_arn(account_id: str):
    return get_role_arn(account_id, DATA_MESH_CONSUMER_ROLENAME)


def get_datamesh_producer_role_arn(account_id: str):
    return get_role_arn(account_id, DATA_MESH_ADMIN_PRODUCER_ROLENAME)


def get_datamesh_consumer_role_arn(account_id: str):
    return get_role_arn(account_id, DATA_MESH_ADMIN_CONSUMER_ROLENAME)


def _validate_credentials(credentials) -> dict:
    if isinstance(credentials, Mapping):
        return credentials
    else:
        # treat as a Boto3 Credentials object
        out = {'AccessKeyId': credentials.access_key, "SecretAccessKey": credentials.secret_key}
        if credentials.token is not None:
            out['SessionToken'] = credentials.token

        return out


def create_session(credentials=None, region=None):
    if credentials is not None:
        use_creds = _validate_credentials(credentials)
        args = {
            "aws_access_key_id": use_creds.get('AccessKeyId'),
            "aws_secret_access_key": use_creds.get('SecretAccessKey')
        }
        if region is not None:
            args["region_name"] = region

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
