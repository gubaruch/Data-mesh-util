import datetime
import time

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
    _current_region = None

    def __init__(self):
        self._iam_client = boto3.client('iam')
        self._sts_client = boto3.client('sts')
        self._current_region = os.getenv('AWS_REGION')

        if self._current_region is None:
            raise Exception("Cannot create a Data Mesh Producer without AWS_REGION environment variable")

    def initialize_producer_account(self, s3_bucket: str, data_mesh_producer_role_arn: str):
        '''
        Sets up an AWS Account to act as a Data Provider into the central Data Mesh Account. This method should be invoked
        by an Administrator of the Producer Account. Creates IAM Role & Policy to get and put restricted S3 Bucket Policies.
        Requires at least 1 S3 Bucket Policy be enabled for future grants.
        :return:
        '''
        self._data_producer_account_id = self._sts_client.get_caller_identity().get('Account')

        # setup the producer IAM role
        utils.configure_iam(
            iam_client=self._iam_client,
            policy_name=PRODUCER_S3_POLICY_NAME,
            policy_desc='IAM Policy enabling Accounts to get and put restricted S3 Bucket Policies',
            policy_template="producer_access_catalog.pystache",
            role_name=DATA_MESH_PRODUCER_ROLENAME,
            role_desc='Role to be used to update S3 Bucket Policies for access by the Data Mesh Account',
            config={"bucket": s3_bucket},
            account_id=self._data_producer_account_id)

        # now create the iam policy allowing the producer group to assume the data mesh producer role
        policy_name = "AssumeDataMeshAdminProducer"
        policy_arn = utils.create_assume_role_policy(
            iam_client=self._iam_client,
            account_id=self._data_producer_account_id,
            policy_name=policy_name,
            role_arn=data_mesh_producer_role_arn
        )

        # now let the group assume the cross account role
        self._iam_client.attach_group_policy(GroupName=("%sGroup" % DATA_MESH_PRODUCER_ROLENAME), PolicyArn=policy_arn)

    def enable_future_sharing(self, s3_bucket: str):
        '''
        Adds a Bucket and Prefix to the policy document for the DataProducer Role, which will enable the Role to potentially
        share the Bucket with the Data Mesh Account in future. This method does not enable access to the Bucket.
        :param s3_bucket:
        :return:
        '''
        # validate that we are being run within the correct account
        if utils.validate_correct_account(self._iam_client, DATA_MESH_PRODUCER_ROLENAME) is False:
            raise Exception("Function should be run in the Data Domain Producer Account")

        self._data_producer_account_id = self._sts_client.get_caller_identity().get('Account')

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

    def grant_datamesh_access_to_s3(self, s3_bucket: str, data_mesh_account_id: str):
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

        # need to grant bucket access to the producer admin role
        data_mesh_producer_role_arn = utils.get_datamesh_producer_role_arn(data_mesh_account_id)

        # generate a new statement for the target bucket policy
        statement_sid = "ReadOnly-%s-%s" % (s3_bucket, data_mesh_producer_role_arn)
        conf = {
            "data_mesh_producer_role_arn": data_mesh_producer_role_arn,
            "bucket": s3_bucket, "sid": statement_sid
        }
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

    def _cleanup_table_def(self, table_def: dict):
        t = table_def.copy()

        def rm(prop):
            del t[prop]

        # remove properties from a TableInfo object returned from get_table to be compatible with put_table
        rm('DatabaseName')
        rm('CreateTime')
        rm('UpdateTime')
        rm('CreatedBy')
        rm('IsRegisteredWithLakeFormation')
        rm('CatalogId')

        return t

    def create_mesh_table(self, table_def: dict, data_mesh_glue_client, data_mesh_lf_client, producer_ram_client,
                          data_mesh_producer_role_arn: str, data_mesh_database_name: str, producer_account_id: str,
                          data_mesh_account_id: str):
        # cleanup the TableInfo object to be usable as a TableInput
        t = self._cleanup_table_def(table_def)

        table_name = t.get('Name')

        # create the glue catalog entry
        try:
            data_mesh_glue_client.create_table(
                DatabaseName=data_mesh_database_name,
                TableInput=t
            )
        except data_mesh_glue_client.exceptions.from_code('AlreadyExistsException'):
            pass

        utils.lf_grant_all(lf_client=data_mesh_lf_client, principal=data_mesh_producer_role_arn,
                           database_name=data_mesh_database_name, table_name=table_name)

        # create a resource link for the data mesh table into the producer account
        link_table_name = "%s_link" % table_name
        try:
            data_mesh_glue_client.create_table(
                DatabaseName=data_mesh_database_name,
                TableInput={"Name": link_table_name,
                            "TargetTable": {"CatalogId": producer_account_id,
                                            "DatabaseName": data_mesh_database_name,
                                            "Name": table_name
                                            }
                            }
            )

            # grant required permissions to the producer account for the resource link
            utils.lf_grant_all(lf_client=data_mesh_lf_client, principal=producer_account_id,
                               database_name=data_mesh_database_name, table_name=link_table_name)

            # in the producer account, accept the RAM share after 1 second - seems to be an async delay
            time.sleep(1)
            utils.accept_pending_lf_resource_share(ram_client=producer_ram_client, sender_account=data_mesh_account_id)
        except data_mesh_glue_client.exceptions.from_code('AlreadyExistsException'):
            pass

        return link_table_name

    def _load_glue_tables(self, glue_client, catalog_id: str, source_db_name: str, table_name_regex: str):
        # get the tables which are included in the set provided through args
        get_tables_args = {
            'CatalogId': catalog_id,
            'DatabaseName': source_db_name
        }

        # add the table filter as a regex matching anything including the provided table
        if table_name_regex is not None:
            get_tables_args['Expression'] = table_name_regex

        finished_reading = False
        last_token = None
        all_tables = []
        while finished_reading is False:
            if last_token is not None:
                get_tables_args['NextToken'] = last_token

            get_table_response = glue_client.get_tables(
                **get_tables_args
            )

            if 'NextToken' in get_table_response:
                last_token = get_table_response.get('NextToken')
            else:
                finished_reading = True

            # add the tables returned from this instance of the request
            if not get_table_response.get('TableList'):
                raise Exception("Unable to find any Tables matching %s in Database %s" % (table_name_regex,
                                                                                          source_db_name))
            else:
                all_tables.extend(get_table_response.get('TableList'))

        return all_tables

    def create_data_products(self, data_mesh_producer_role_arn: str, source_database_name: str,
                             table_name_regex: str = None, sync_mesh_catalog_schedule: str = None,
                             sync_mesh_crawler_role_arn: str = None):
        '''
        Creates a copy of a local set of objects within the data mesh account and sets up synchronisation between the
        two accounts.
        :param data_mesh_producer_role_arn:
        :param source_database_name:
        :param target_database_name:
        :param table_name_regex:
        :return:
        '''
        # assume the data mesh admin producer role. if this fails, then the requesting identity is wrong
        current_account = self._sts_client.get_caller_identity()
        session_name = "%s-%s-%s" % (current_account.get('UserId'), current_account.get(
            'Account'), datetime.datetime.now().strftime("%Y-%m-%d"))
        data_mesh_sts_session = self._sts_client.assume_role(RoleArn=data_mesh_producer_role_arn,
                                                             RoleSessionName=session_name)

        # generate the target database name for the mesh
        data_mesh_database_name = "%s-%s" % (source_database_name, current_account.get('Account'))

        # generate a reference to the current account data producer role arn
        producer_role_arn = utils.get_producer_role_arn(
            account_id=current_account.get('Account')
        )

        # parse what the data mesh account ID is for later use
        data_mesh_account_id = data_mesh_sts_session.get('AssumedRoleUser').get('Arn').split(':')[4]

        # create clients for the current account and with the new credentials in the data mesh account
        producer_glue_client = boto3.client('glue', region_name=self._current_region)
        producer_lf_client = boto3.client('lakeformation', region_name=self._current_region)
        producer_ram_client = boto3.client('ram', region_name=self._current_region)
        data_mesh_glue_client = utils.generate_client(service='glue', region=self._current_region,
                                                      credentials=data_mesh_sts_session.get('Credentials'))
        data_mesh_lf_client = utils.generate_client(service='lakeformation', region=self._current_region,
                                                    credentials=data_mesh_sts_session.get('Credentials'))

        # load the specified tables to be created as data products
        all_tables = self._load_glue_tables(
            glue_client=producer_glue_client,
            catalog_id=current_account.get('Account'),
            source_db_name=source_database_name,
            table_name_regex=table_name_regex
        )

        # get or create the target database exists in the mesh account
        utils.get_or_create_database(
            glue_client=data_mesh_glue_client,
            database_name=data_mesh_database_name,
            database_desc="Database to contain objects from Source Database %s.%s" % (
                current_account.get('Account'), source_database_name)
            , default_principal=data_mesh_producer_role_arn
        )

        # grant the data mesh admin producer all permissions on this database
        utils.lf_grant_all(lf_client=data_mesh_lf_client, principal=data_mesh_producer_role_arn,
                           database_name=data_mesh_database_name)

        # get or create a data mesh shared database in the producer account
        utils.get_or_create_database(
            glue_client=producer_glue_client,
            database_name=data_mesh_database_name,
            database_desc="Database to contain objects objects shared with the Data Mesh Account",
            default_principal=producer_role_arn
        )

        # grant the producer all permissions on this database
        # utils.lf_grant_all(lf_client=producer_lf_client, principal=producer_role_arn,
        #                    database_name=data_mesh_database_name)

        for table in all_tables:
            table_s3_path = table.get('StorageDescriptor').get('Location')

            # create a mesh table for the local copy
            created_table = self.create_mesh_table(
                table_def=table,
                data_mesh_glue_client=data_mesh_glue_client,
                data_mesh_lf_client=data_mesh_lf_client,
                producer_ram_client=producer_ram_client,
                data_mesh_producer_role_arn=data_mesh_producer_role_arn,
                data_mesh_database_name=data_mesh_database_name,
                producer_account_id=current_account.get('Account'),
                data_mesh_account_id=data_mesh_account_id
            )

            if sync_mesh_catalog_schedule is not None:
                utils.create_crawler(
                    glue_client=producer_glue_client,
                    database_name=data_mesh_database_name,
                    table_name=created_table,
                    s3_location=table_s3_path,
                    crawler_role=sync_mesh_crawler_role_arn,
                    sync_schedule=sync_mesh_catalog_schedule
                )
