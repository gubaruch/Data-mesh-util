try:
    from collections.abc import Mapping  # noqa
except ImportError:
    from collections import Mapping  # noqa

from data_mesh_util.lib.constants import *
import json
import os
import pystache
import time
import botocore
import boto3
import datetime


def make_iam_session_name(current_account):
    return "%s-%s-%s" % (current_account.get('UserId').replace(":", ""), current_account.get(
        'Account'), datetime.datetime.now().strftime("%Y-%m-%d"))


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


def add_aws_trust_to_role(iam_client, account_id: str, role_name: str):
    '''
    Private method to add a trust relationship to an AWS Account to a Role
    :return:
    '''
    # update the  trust policy to include the provided account ID
    response = iam_client.get_role(RoleName=role_name)

    policy_doc = response.get('Role').get('AssumeRolePolicyDocument')

    # add the account to the trust relationship
    trusted_entities = policy_doc.get('Statement')[0].get('Principal').get('AWS')
    if account_id not in trusted_entities:
        trusted_entities.append(account_id)
        policy_doc.get('Statement')[0].get('Principal')['AWS'] = trusted_entities

    iam_client.update_assume_role_policy(RoleName=role_name, PolicyDocument=json.dumps(policy_doc))


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


def configure_iam(iam_client, policy_name: str, policy_desc: str, policy_template: str,
                  role_name: str, role_desc: str, account_id: str, logger, config: dict = None,
                  additional_assuming_principals: dict = None, managed_policies_to_attach: list = None):
    policy_arn = None
    try:
        # create an IAM Policy from the template
        policy_doc = generate_policy(policy_template, config)

        response = iam_client.create_policy(
            PolicyName=policy_name,
            Path=DATA_MESH_IAM_PATH,
            PolicyDocument=policy_doc,
            Description=policy_desc,
            Tags=DEFAULT_TAGS
        )
        policy_arn = response.get('Policy').get('Arn')
        waiter = iam_client.waiter('policy_exists')
        waiter.wait(PolicyArn=policy_arn)
    except iam_client.exceptions.EntityAlreadyExistsException:
        policy_arn = "arn:aws:iam::%s:policy%s%s" % (account_id, DATA_MESH_IAM_PATH, policy_name)

    logger.info(f"Policy {policy_name} validated as {policy_arn}")

    # create a non-root user who can assume the role
    try:
        response = iam_client.create_user(
            Path=DATA_MESH_IAM_PATH,
            UserName=role_name,
            Tags=DEFAULT_TAGS
        )
        logger.info(f"Created new User {role_name}")

        waiter = iam_client.get_waiter('user_exists')
        waiter.wait(UserName=role_name)
    except iam_client.exceptions.EntityAlreadyExistsException:
        pass

    user_arn = "arn:aws:iam::%s:user%s%s" % (account_id, DATA_MESH_IAM_PATH, role_name)

    # create a group for the user
    group_name = f"{role_name}Group"
    try:
        response = iam_client.create_group(
            Path=DATA_MESH_IAM_PATH,
            GroupName=group_name
        )
        logger.info(f"Created new Group {group_name}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        pass

    group_arn = "arn:aws:iam::%s:group%s%sGroup" % (account_id, DATA_MESH_IAM_PATH, role_name)

    # put the user into the group
    try:
        response = iam_client.add_user_to_group(
            GroupName=group_name,
            UserName=role_name
        )
        logger.info(f"Added User {role_name} to Group {group_name}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        pass

    role_arn = None
    try:
        # now create the IAM Role with a trust policy to the indicated principal and the root user
        aws_principals = [user_arn, ("arn:aws:iam::%s:root" % account_id)]

        role_response = iam_client.create_role(
            Path=DATA_MESH_IAM_PATH,
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(
                create_assume_role_doc(aws_principals=aws_principals,
                                       additional_principals=additional_assuming_principals)),
            Description=role_desc,
            Tags=DEFAULT_TAGS
        )

        role_arn = role_response.get('Role').get('Arn')

        # wait for role active
        waiter = iam_client.waiter('role_exists')
        waiter.wait(RoleName=role_name)
    except iam_client.exceptions.EntityAlreadyExistsException:
        role_arn = iam_client.get_role(RoleName=role_name).get(
            'Role').get('Arn')

    logger.info(f"Validated Role {role_name} as {role_arn}")

    # attach the created policy to the role
    iam_client.attach_role_policy(
        RoleName=role_name,
        PolicyArn=policy_arn
    )
    logger.info(f"Attached Policy {policy_arn} to {role_name}")

    # attach the indicated managed policies
    if managed_policies_to_attach:
        for policy in managed_policies_to_attach:
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn="arn:aws:iam::aws:policy/%s" % policy
            )
            logger.info(f"Attached managed policy {policy}")

    create_assume_role_policy(iam_client=iam_client, account_id=account_id, policy_name=("Assume%s" % role_name),
                              role_arn=role_arn, logger=logger)

    # now let the group assume the role
    iam_client.attach_group_policy(GroupName=group_name, PolicyArn=policy_arn)
    logger.info(f"Bound {policy_arn} to Group {group_name}")

    # TODO Grant permissions for IamAllowedPrincipals to SUPER for this Account
    return role_arn, user_arn, group_arn


def flatten_default_tags():
    output = {}
    for t in DEFAULT_TAGS:
        output[t.get('Key')] = t.get('Value')

    return output


def get_or_create_database(glue_client, database_name: str, database_desc: str):
    database_exists = None
    try:
        database_exists = glue_client.get_database(
            Name=database_name
        )
    except glue_client.exceptions.from_code('EntityNotFoundException'):
        pass

    if database_exists is None or 'Database' not in database_exists:
        # create the database
        glue_client.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": database_desc,
            }
        )


def create_assume_role_policy(iam_client, account_id: str, policy_name: str, role_arn: str, logger):
    # create a policy that lets someone assume this new role
    policy_arn = None
    try:
        response = iam_client.create_policy(
            PolicyName=policy_name,
            Path=DATA_MESH_IAM_PATH,
            PolicyDocument=json.dumps(create_assume_role_doc(resource=role_arn)),
            Description=("Policy allowing the grantee the ability to assume Role %s" % role_arn),
            Tags=DEFAULT_TAGS
        )
        policy_arn = response.get('Policy').get('Arn')
    except iam_client.exceptions.EntityAlreadyExistsException:
        policy_arn = "arn:aws:iam::%s:policy%s%s" % (account_id, DATA_MESH_IAM_PATH, policy_name)

    logger.info(f"Validated {policy_name} as {policy_arn}")

    return policy_arn


def _get_role_arn(account_id: str, role_name: str):
    return "arn:aws:iam::%s:role%s%s" % (account_id, DATA_MESH_IAM_PATH, role_name)


def get_producer_role_arn(account_id: str):
    return _get_role_arn(account_id, DATA_MESH_PRODUCER_ROLENAME)


def get_consumer_role_arn(account_id: str):
    return _get_role_arn(account_id, DATA_MESH_CONSUMER_ROLENAME)


def get_datamesh_producer_role_arn(account_id: str):
    return _get_role_arn(account_id, DATA_MESH_ADMIN_PRODUCER_ROLENAME)


def get_datamesh_consumer_role_arn(account_id: str):
    return _get_role_arn(account_id, DATA_MESH_ADMIN_CONSUMER_ROLENAME)


def _validate_credentials(credentials) -> dict:
    if isinstance(credentials, Mapping):
        return credentials
    else:
        # treat as a Boto3 Credentials object
        out = {'AccessKeyId': credentials.access_key, "SecretAccessKey": credentials.secret_key}
        if credentials.token is not None:
            out['SessionToken'] = credentials.token

        return out


def load_client_info_from_file(from_path: str, region_name: str):
    if from_path is None:
        raise Exception("Unable to load Client Connection information from None file")
    _creds = None

    with open(from_path, 'r') as w:
        _creds = json.load(w)
        w.close()

    _clients = {}
    _account_ids = {}
    _credentials_dict = {}

    for token in [MESH, PRODUCER, CONSUMER]:
        _clients[token] = generate_client('sts', region=region_name, credentials=_creds.get(token))
        _account_ids[token] = _creds.get(token).get('AccountId')
        _credentials_dict = _creds

    return _clients, _account_ids, _credentials_dict


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


def lf_grant_permissions(logger, lf_client, principal: str, database_name: str, table_name: str = None,
                         permissions: list = ['ALL'],
                         grantable_permissions: list = ['ALL']):
    try:
        table_spec = {
            'DatabaseName': database_name
        }
        if table_name is None or table_name == "*":
            table_spec['TableWildcard'] = {}
        else:
            table_spec['Name'] = table_name

        # always grant describe even if not requested
        if 'DESCRIBE' not in permissions:
            permissions.append('DESCRIBE')

        args = {
            "Principal": {
                'DataLakePrincipalIdentifier': principal
            },
            "Resource": {
                'Table': table_spec
            },
            "Permissions": permissions
        }

        if grantable_permissions is not None:
            args["PermissionsWithGrantOption"] = grantable_permissions

        logger.debug(args)

        response = lf_client.grant_permissions(**args)

        logger.info(f"Granted LakeFormation Permissions {permissions} on {database_name}.{table_name} to {principal}")

        return response
    except lf_client.exceptions.from_code('AlreadyExistsException') as aee:
        return None
    except lf_client.exceptions.InvalidInputException as iie:
        if "Permissions modification is invalid" in str(iie):
            # this is an error thrown when you try to create the same permissions that already exist :(
            return None
        elif "Please revoke permission(s) for IAM_ALLOWED_PRINCIPALS on the table" in str(iie):
            # this occurs because we are granting any IAM principal to describe the table, which means that the previous creation of the grant is already in place. ignore
            return None
        else:
            logger.error(
                f"Exception while granting LakeFormation Permissions {permissions} on {database_name}.{table_name} to {principal}")
            raise iie
    except Exception as eve:
        logger.error(eve)
        print(eve)

        raise eve


def accept_pending_lf_resource_share(logger, ram_client, sender_account: str):
    accepted_one = False
    get_response = ram_client.get_resource_share_invitations(
    )

    for r in get_response.get('resourceShareInvitations'):
        # only accept lakeformation shares
        if r.get('senderAccountId') == sender_account and 'LakeFormation' in r.get('resourceShareName') and r.get(
                'status') == 'PENDING':
            ram_client.accept_resource_share_invitation(
                resourceShareInvitationArn=r.get('resourceShareInvitationArn')
            )
            accepted_one = True
            logger.info("Accepted RAM Share")


def create_crawler(glue_client, crawler_role: str, database_name: str, table_name: str, s3_location: str,
                   sync_schedule: str, enable_lineage: bool = True):
    crawler_name = '%s-%s' % (database_name, table_name)
    try:
        glue_client.get_crawler(Name=crawler_name)
    except glue_client.exceptions.from_code('EntityNotFoundException'):
        glue_client.create_crawler(
            Name=crawler_name,
            Role=crawler_role,
            DatabaseName=database_name,
            Description="S3 Crawler to sync structure of %s.%s to Data Mesh" % (database_name, table_name),
            Targets={
                'S3Targets': [
                    {
                        'Path': s3_location
                    },
                ]
            },
            Schedule="cron(0 */4 * * ? *)" if sync_schedule is None else sync_schedule,
            SchemaChangePolicy={
                'UpdateBehavior': 'LOG',
                'DeleteBehavior': 'LOG'
            },
            RecrawlPolicy={
                'RecrawlBehavior': 'CRAWL_NEW_FOLDERS_ONLY'
            },
            LineageConfiguration={
                'CrawlerLineageSettings': 'ENABLE' if enable_lineage is True else 'DISABLE'
            },
            Tags=flatten_default_tags()
        )

    return crawler_name
