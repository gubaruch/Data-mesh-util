from constants import *
import json
import os
import pystache
import time


def validate_correct_account(iam_client, role_must_exist: str):
    try:
        iam_client.get_role(RoleName=role_must_exist)
        return True
    except iam_client.exceptions.NoSuchEntityException:
        return False


def generate_policy(template_file: str, config: dict):
    with open("%s/%s" % (os.path.join(os.path.dirname(__file__), "../resource"), template_file)) as t:
        template = t.read()

    rendered = pystache.Renderer().render(template, config)

    return rendered


def get_assume_role_doc(principals: list = None, resource: str = None):
    document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
            }
        ]
    }

    if principals is not None:
        document.get('Statement')[0]['Principal'] = {"AWS": principals}

    if resource is not None:
        document.get('Statement')[0]['Resource'] = resource

    return document


def configure_iam(iam_client, policy_name: str, policy_desc: str, policy_template: str,
                  role_name: str, role_desc: str, account_id: str, config: dict = None):
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
    except iam_client.exceptions.EntityAlreadyExistsException:
        policy_arn = "arn:aws:iam::%s:policy%s%s" % (account_id, DATA_MESH_IAM_PATH, policy_name)

    # create a non-root user who can assume the role
    try:
        response = iam_client.create_user(
            Path=DATA_MESH_IAM_PATH,
            UserName=role_name,
            Tags=DEFAULT_TAGS
        )

        # have to sleep for a second here, as there appears to be eventual consistency between create_user and create_role
        time.sleep(.5)
    except iam_client.exceptions.EntityAlreadyExistsException:
        pass

    user_arn = "arn:aws:iam::%s:user%s%s" % (account_id, DATA_MESH_IAM_PATH, role_name)

    # create a group for the user
    try:
        response = iam_client.create_group(
            Path=DATA_MESH_IAM_PATH,
            GroupName=("%sGroup" % role_name)
        )
    except iam_client.exceptions.EntityAlreadyExistsException:
        pass

    group_arn = "arn:aws:iam::%s:group%s%sGroup" % (account_id, DATA_MESH_IAM_PATH, role_name)

    # put the user into the group
    try:
        response = iam_client.add_user_to_group(
            GroupName=("%sGroup" % role_name),
            UserName=role_name
        )
    except iam_client.exceptions.EntityAlreadyExistsException:
        pass

    role_arn = None
    try:
        # now create the IAM Role with a trust policy to the indicated principal and the root user
        role_response = iam_client.create_role(
            Path=DATA_MESH_IAM_PATH,
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(
                get_assume_role_doc(principals=[user_arn, ("arn:aws:iam::%s:root" % account_id)])),
            Description=role_desc,
            Tags=DEFAULT_TAGS
        )

        role_arn = role_response.get('Role').get('Arn')

        # attach the created policy to the role
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
    except iam_client.exceptions.EntityAlreadyExistsException:
        role_arn = iam_client.get_role(RoleName=role_name).get(
            'Role').get('Arn')

    # create a policy that lets someone assume this new role
    policy_arn = None
    try:
        response = iam_client.create_policy(
            PolicyName=("Assume%s" % role_name),
            Path=DATA_MESH_IAM_PATH,
            PolicyDocument=json.dumps(get_assume_role_doc(resource=role_arn)),
            Description=("Policy allowing the grantee the ability to assume the %s Role" % role_name),
            Tags=DEFAULT_TAGS
        )
        policy_arn = response.get('Policy').get('Arn')
    except iam_client.exceptions.EntityAlreadyExistsException:
        policy_arn = "arn:aws:iam::%s:policy%s%s" % (account_id, DATA_MESH_IAM_PATH, ("Assume%s" % role_name))

    # now let the group assume the role
    iam_client.attach_group_policy(GroupName=("%sGroup" % role_name), PolicyArn=policy_arn)

    return role_arn
