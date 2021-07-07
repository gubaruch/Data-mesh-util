from constants import *
import json
import os
import pystache


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


def get_assume_role_doc(principal):
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


def create_role_and_attach_policy(iam_client, policy_name: str, policy_desc: str, policy_template: str,
                                  role_name: str, role_desc: str, account_id: str, config: dict = None):
    # create an IAM Policy from the template
    policy_doc = generate_policy(policy_template, config)

    try:
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

    role_arn = None
    try:
        # now create the IAM Role
        role_response = iam_client.create_role(
            Path=DATA_MESH_IAM_PATH,
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(
                get_assume_role_doc(principal="arn:aws:iam::%s:root" % account_id)),
            Description=role_desc,
            Tags=DEFAULT_TAGS
        )

        role_arn = role_response.get('Role').get('Arn')
    except iam_client.exceptions.EntityAlreadyExistsException:
        role_arn = iam_client.get_role(RoleName=role_name).get(
            'Role').get('Arn')

    # attach the created policy to the role
    iam_client.attach_role_policy(
        RoleName=role_name,
        PolicyArn=policy_arn
    )

    return role_arn
