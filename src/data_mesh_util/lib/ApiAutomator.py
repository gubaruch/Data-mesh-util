import sys
import logging
import time

import boto3
import botocore.exceptions
import shortuuid

from data_mesh_util.lib.constants import *
import json
import data_mesh_util.lib.utils as utils


class ApiAutomator:
    _target_account = None
    _session = None
    _logger = None
    _region = None
    _logger = logging.getLogger("ApiAutomator")
    # make sure we always log to standard out
    _logger.addHandler(logging.StreamHandler(sys.stdout))
    _clients = None

    def __init__(self, target_account: str, session: boto3.session.Session, log_level: str = "INFO"):
        self._target_account = target_account
        self._session = session
        self._logger.setLevel(log_level)
        self._clients = {}

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

    def add_aws_trust_to_role(self, account_id_to_trust: str, trust_role_name: str, update_role_name: str):
        '''
        Method to add a trust relationship to an AWS Account to a Role
        :return:
        '''
        iam_client = self._get_client('iam')

        # update the  trust policy to include the provided account ID
        response = iam_client.get_role(RoleName=update_role_name)

        policy_doc = response.get('Role').get('AssumeRolePolicyDocument')

        trust_role_name = utils.get_role_arn(account_id=account_id_to_trust, role_name=trust_role_name)
        # add the account to the trust relationship
        trusted_entities = policy_doc.get('Statement')[0].get('Principal').get('AWS')
        if account_id_to_trust not in trusted_entities:
            trusted_entities.append(trust_role_name)
            policy_doc.get('Statement')[0].get('Principal')['AWS'] = trusted_entities

        print(policy_doc)
        iam_client.update_assume_role_policy(RoleName=update_role_name, PolicyDocument=json.dumps(policy_doc))

        self._logger.info("Enabled Account %s to assume %s" % (account_id_to_trust, update_role_name))

    def _validate_tag(self, tag_key: str, tag_body: dict) -> None:
        lf_client = self._get_client('lakeformation')

        # create the tag or validate it exists
        try:
            lf_client.create_lf_tag(
                TagKey=tag_key,
                TagValues=tag_body.get('ValidValues')
            )
        except lf_client.exceptions.AlreadyExistsException:
            pass
        except lf_client.exceptions.InvalidInputException as e:
            if 'Tag key already exists' in str(e):
                pass
            else:
                raise e

    def attach_tag(self, database: str, table: str, tag: tuple):
        # create the tag or make sure it already exists
        tag_key = tag[0]
        tag_body = tag[1]
        self._validate_tag(tag_key=tag_key, tag_body=tag_body)

        # attach the tag to the table
        lf_client = self._get_client('lakeformation')
        try:
            args = {
                "Resource": {
                    'Table': {
                        'DatabaseName': database,
                        'Name': table
                    }
                },
                "LFTags": [
                    {
                        'TagKey': tag_key,
                        'TagValues': tag_body.get('TagValues')
                    },
                ]
            }
            response = lf_client.add_lf_tags_to_resource(**args)
        except lf_client.exceptions.AlreadyExistsException:
            pass

    def configure_iam(self, policy_name: str, policy_desc: str, policy_template: str, role_name: str, role_desc: str,
                      account_id: str, data_mesh_account_id: str, config: dict = None,
                      additional_assuming_principals: dict = None, managed_policies_to_attach: list = None):
        iam_client = self._get_client('iam')

        policy_arn = None
        try:
            # create an IAM Policy from the template
            policy_doc = utils.generate_policy(policy_template, config)

            response = iam_client.create_policy(
                PolicyName=policy_name,
                Path=DATA_MESH_IAM_PATH,
                PolicyDocument=policy_doc,
                Description=policy_desc,
                Tags=DEFAULT_TAGS
            )
            policy_arn = response.get('Policy').get('Arn')
            waiter = iam_client.get_waiter('policy_exists')
            waiter.wait(PolicyArn=policy_arn)
        except iam_client.exceptions.EntityAlreadyExistsException:
            policy_arn = utils.get_policy_arn(account_id, policy_name)

        self._logger.info(f"Policy {policy_name} validated as {policy_arn}")

        # create a non-root user who can assume the role
        try:
            response = iam_client.create_user(
                Path=DATA_MESH_IAM_PATH,
                UserName=role_name,
                Tags=DEFAULT_TAGS
            )
            self._logger.info(f"Created new User {role_name}")

            waiter = iam_client.get_waiter('user_exists')
            waiter.wait(UserName=role_name)
        except iam_client.exceptions.EntityAlreadyExistsException:
            self._logger.info(f"User {role_name} already exists. No action required.")

        user_arn = "arn:aws:iam::%s:user%s%s" % (account_id, DATA_MESH_IAM_PATH, role_name)

        # create a group for the user
        group_name = f"{role_name}Group"
        try:
            response = iam_client.create_group(
                Path=DATA_MESH_IAM_PATH,
                GroupName=group_name
            )
            self._logger.info(f"Created new Group {group_name}")
        except iam_client.exceptions.EntityAlreadyExistsException:
            self._logger.info(f"Group {group_name} already exists. No action required.")

        group_arn = "arn:aws:iam::%s:group%s%sGroup" % (account_id, DATA_MESH_IAM_PATH, role_name)

        # put the user into the group
        try:
            response = iam_client.add_user_to_group(
                GroupName=group_name,
                UserName=role_name
            )
            self._logger.info(f"Added User {role_name} to Group {group_name}")
        except iam_client.exceptions.EntityAlreadyExistsException:
            self._logger.info(f"User {role_name} already in {group_name}. No action required.")

        role_arn = None

        self._logger.debug("Waiting for User to be ready for inclusion in AssumeRolePolicy")
        time.sleep(1)

        try:
            # now create the IAM Role with a trust policy to the indicated principal and the root user
            aws_principals = [user_arn, ("arn:aws:iam::%s:root" % account_id)]
            iam_client.create_role(
                Path=DATA_MESH_IAM_PATH,
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(
                    utils.create_assume_role_doc(aws_principals=aws_principals,
                                                 additional_principals=additional_assuming_principals)),
                Description=role_desc,
                Tags=DEFAULT_TAGS
            )
            # wait for role active
            waiter = iam_client.get_waiter('role_exists')
            waiter.wait(RoleName=role_name)

            role_arn = utils.get_role_arn(account_id, role_name)
        except iam_client.exceptions.EntityAlreadyExistsException:
            role_arn = iam_client.get_role(RoleName=role_name).get(
                'Role').get('Arn')

        self._logger.info(f"Validated Role {role_name} as {role_arn}")
        self._logger.debug("Waiting for Role to be ready for Policy Attach")
        time.sleep(1)

        # attach the created policy to the role
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
        self._logger.info(f"Attached Policy {policy_arn} to {role_name}")

        # attach the indicated managed policies
        if managed_policies_to_attach:
            for policy in managed_policies_to_attach:
                iam_client.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn="arn:aws:iam::aws:policy/%s" % policy
                )
                self._logger.info(f"Attached managed policy {policy}")

        # create an assume role policy
        policy_arn = self.create_assume_role_policy(
            source_account_id=account_id,
            policy_name=("Assume%s" % role_name),
            role_arn=role_arn
        )

        # now let the group assume the role
        iam_client.attach_group_policy(GroupName=group_name, PolicyArn=policy_arn)
        self._logger.info(f"Bound {policy_arn} to Group {group_name}")

        # let the role assume the read only consumer policy
        if account_id == data_mesh_account_id:
            iam_client.attach_role_policy(RoleName=role_name,
                                          PolicyArn=utils.get_policy_arn(data_mesh_account_id,
                                                                         f"Assume{DATA_MESH_READONLY_ROLENAME}"))

        return role_arn, user_arn, group_arn

    def create_assume_role_policy(self, source_account_id: str, policy_name: str, role_arn: str):
        iam_client = self._get_client('iam')

        # create a policy that lets someone assume this new role
        policy_arn = None
        try:
            response = iam_client.create_policy(
                PolicyName=policy_name,
                Path=DATA_MESH_IAM_PATH,
                PolicyDocument=json.dumps(utils.create_assume_role_doc(resource=role_arn)),
                Description=("Policy allowing the grantee the ability to assume Role %s" % role_arn),
                Tags=DEFAULT_TAGS
            )
            policy_arn = response.get('Policy').get('Arn')
        except iam_client.exceptions.EntityAlreadyExistsException:
            policy_arn = "arn:aws:iam::%s:policy%s%s" % (source_account_id, DATA_MESH_IAM_PATH, policy_name)

        self._logger.info(f"Validated {policy_name} as {policy_arn}")

        return policy_arn

    def leave_ram_shares(self, principal: str, ram_shares: dict) -> None:
        ram_client = self._get_client('ram')

        for object, share_info in ram_shares.items():
            ram_client.disassociate_resource_share(
                resourceShareArn=share_info.get('arn'),
                principals=[
                    principal,
                ]
            )

    def lf_grant_permissions(self, data_mesh_account_id: str, principal: str, database_name: str,
                             table_name: str = None,
                             permissions: list = ['ALL'],
                             grantable_permissions: list = ['ALL']):
        lf_client = self._get_client('lakeformation')

        try:
            args = {
                "CatalogId": data_mesh_account_id,
                "Principal": {
                    'DataLakePrincipalIdentifier': principal
                },
                "Permissions": permissions
            }

            if table_name is not None:
                db_spec = {
                    'CatalogId': data_mesh_account_id,
                    'DatabaseName': database_name
                }
                if table_name == "*":
                    db_spec['TableWildcard'] = {}
                else:
                    db_spec['Name'] = table_name

                args["Resource"] = {
                    'Table': db_spec
                }
            else:
                # create a database grant
                args['Resource'] = {
                    'Database': {
                        'CatalogId': data_mesh_account_id,
                        'Name': database_name
                    }
                }

            # always grant describe even if not requested
            if 'DESCRIBE' not in permissions:
                permissions.append('DESCRIBE')

            if grantable_permissions is not None:
                args["PermissionsWithGrantOption"] = grantable_permissions

            self._logger.debug(args)

            response = lf_client.grant_permissions(**args)

            report_t = ""
            if table_name is not None:
                report_t = f".{table_name}"

            self._logger.info(
                f"Granted LakeFormation Permissions {permissions} on {database_name}{report_t} to {principal}")

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
                self._logger.error(
                    f"Exception while granting LakeFormation Permissions {permissions} on {database_name}.{table_name} to {principal}")
                raise iie
        except Exception as eve:
            self._logger.error(eve)
            print(eve)

            raise eve

    def create_crawler(self, crawler_role: str, database_name: str, table_name: str, s3_location: str,
                       sync_schedule: str, enable_lineage: bool = True):
        glue_client = self._get_client('glue')
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
                Tags=utils.flatten_default_tags()
            )
            self._logger.info("Created new Glue Crawler %s" % crawler_name)

        # create lakeformation permissions in the mesh account for the glue crawler role

        # create s3 permission for glue crawler role

        return crawler_name

    def create_remote_table(self, data_mesh_account_id: str,
                            database_name: str,
                            local_table_name: str,
                            remote_table_name: str) -> None:
        try:
            glue_client = self._get_client(('glue'))
            glue_client.create_table(
                DatabaseName=database_name,
                TableInput={"Name": local_table_name,
                            "TargetTable": {"CatalogId": data_mesh_account_id,
                                            "DatabaseName": database_name,
                                            "Name": remote_table_name
                                            }
                            }
            )
            self._logger.info(f"Created Resource Link Table {local_table_name}")
        except glue_client.exceptions.from_code('AlreadyExistsException'):
            self._logger.info(f"Resource Link Table {local_table_name} Already Exists")

    def get_or_create_database(self, database_name: str, database_desc: str, source_account: str = None):
        glue_client = self._get_client('glue')

        args = {
            "DatabaseInput": {
                "Name": database_name,
                "Description": database_desc,
            }
        }

        if source_account is not None:
            args['DatabaseInput']['TargetDatabase'] = {
                "CatalogId": source_account,
                "DatabaseName": database_name
            }
            del args['DatabaseInput']["Description"]

        # create the database
        try:
            glue_client.create_database(
                **args
            )
        except glue_client.exceptions.AlreadyExistsException:
            pass

        self._logger.info(f"Verified Database {database_name}")

    def set_default_db_permissions(self, database_name: str):
        glue_client = self._get_client('glue')

        glue_client.update_database(
            CatalogId=self._target_account,
            Name=database_name,
            DatabaseInput={
                "Name": database_name,
                "CreateTableDefaultPermissions": []
            }
        )

    def set_default_lf_permissions(self):
        # remove default IAM settings in lakeformation for the account, and setup the manager role and this caller as admins
        lf_client = self._get_client('lakeformation')
        settings = lf_client.get_data_lake_settings().get('DataLakeSettings')
        settings['CreateTableDefaultPermissions'] = []
        lf_client.put_data_lake_settings(DataLakeSettings=settings)

    def add_datalake_admin(self, principal: str):
        lf_client = self._get_client('lakeformation')

        admins = lf_client.get_data_lake_settings().get('DataLakeSettings').get("DataLakeAdmins")

        admins.append({
            'DataLakePrincipalIdentifier': principal
        })
        # Horrible retry logic required to avoid boto3 exception using a role as a principal too soon after it's been created
        retries = 0
        while True:
            try:
                lf_client.put_data_lake_settings(
                    DataLakeSettings={
                        'DataLakeAdmins': admins
                    }
                )
            except lf_client.exceptions.InvalidInputException:
                self._logger.info(f"Error setting DataLakeAdmins as {admins}. Backing off....")
                retries += 1
                if retries > 5:
                    raise
                time.sleep(3)
                continue
            break

    def _get_s3_path_prefix(self, prefix: str) -> str:
        return prefix.replace(f"s3://{self._get_bucket_name(prefix)}", "")

    def _transform_bucket_policy(self, bucket_policy: dict, principal_account: str,
                                 access_path: str) -> dict:
        use_bucket_name = self._get_bucket_name(access_path)
        policy_sid = f"{BUCKET_POLICY_STATEMENT_SID}-{use_bucket_name}"

        # generate a new bucket policy from the template
        s3_path = self._get_s3_path_prefix(access_path)
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
                    self._logger.info(
                        f"Not modifying bucket policy as principal {principal_account} has already been added")

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

    def accept_pending_lf_resource_shares(self, sender_account: str, filter_resource_arn: str = None):
        ram_client = self._get_client('ram')

        get_response = ram_client.get_resource_share_invitations()

        accepted_share = False
        for r in get_response.get('resourceShareInvitations'):
            # only accept peding lakeformation shares from the source account
            if r.get('senderAccountId') == sender_account and 'LakeFormation' in r.get('resourceShareName') and r.get(
                    'status') == 'PENDING':
                if filter_resource_arn is None or r.get('resourceShareArn') == filter_resource_arn:
                    ram_client.accept_resource_share_invitation(
                        resourceShareInvitationArn=r.get('resourceShareInvitationArn')
                    )
                    accepted_share = True
                    self._logger.info(f"Accepted RAM Share {r.get('resourceShareInvitationArn')}")

        if accepted_share is False:
            self._logger.info("No Pending RAM Shares to Accept")
