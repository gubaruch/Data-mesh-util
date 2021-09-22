import datetime
import time
import boto3
import os
import sys
import json

import botocore.session
import shortuuid
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), "resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))

from data_mesh_util.lib.constants import *
import data_mesh_util.lib.utils as utils
from data_mesh_util.lib.SubscriberTracker import *


class DataMeshProducer:
    _data_mesh_account_id = None
    _data_producer_account_id = None
    _data_consumer_account_id = None
    _data_mesh_manager_role_arn = None
    _session = None
    _iam_client = None
    _sts_client = None
    _config = {}
    _current_region = None
    _logger = logging.getLogger("DataMeshProducer")
    stream_handler = logging.StreamHandler(sys.stdout)
    _logger.addHandler(stream_handler)
    _data_mesh_account_id = None
    _data_producer_role_arn = None
    _data_mesh_sts_session = None
    _subscription_tracker = None
    _current_account = None

    def __init__(self, data_mesh_account_id: str, log_level: str = "INFO", use_credentials=None):
        self._data_mesh_account_id = data_mesh_account_id

        self._current_region = os.getenv('AWS_REGION')
        if self._current_region is None:
            raise Exception("Cannot create a Data Mesh Producer without AWS_REGION environment variable")

        if use_credentials is not None:
            self._session = utils.create_session(credentials=use_credentials, region=self._current_region)
            self._iam_client = self._session.client('iam')
            self._sts_client = self._session.client('sts')
        else:
            self._session = botocore.session.get_session()
            self._iam_client = boto3.client('iam')
            self._sts_client = boto3.client('sts')

        self._logger.setLevel(log_level)

        self._current_account = self._sts_client.get_caller_identity()
        session_name = utils.make_iam_session_name(self._current_account)
        self._data_producer_role_arn = utils.get_datamesh_producer_role_arn(account_id=data_mesh_account_id)
        self._data_mesh_sts_session = self._sts_client.assume_role(RoleArn=self._data_producer_role_arn,
                                                                   RoleSessionName=session_name)
        self._logger.debug("Created new STS Session for Data Mesh Admin Producer")
        self._logger.debug(self._data_mesh_sts_session)

        # validate that we are running in the data mesh account
        utils.validate_correct_account(self._data_mesh_sts_session.get('Credentials'), self._data_mesh_account_id)

        self._subscription_tracker = SubscriberTracker(credentials=self._data_mesh_sts_session.get('Credentials'),
                                                       data_mesh_account_id=data_mesh_account_id,
                                                       region_name=self._current_region,
                                                       log_level=log_level)

    # TODO Deprecate this method as we don't need it due to using lakeformation permissions
    def enable_future_sharing(self, s3_bucket: str):
        '''
        Adds a Bucket and Prefix to the policy document for the DataProducer Role, which will enable the Role to potentially
        share the Bucket with the Data Mesh Account in future. This method does not enable access to the Bucket.
        :param s3_bucket:
        :return:
        '''
        # get the producer policy
        arn = "arn:aws:iam::%s:policy%s%s" % (
            self._data_producer_account_id, DATA_MESH_IAM_PATH, PRODUCER_POLICY_NAME)
        policy_version = self._iam_client.get_policy(PolicyArn=arn).get('Policy').get('DefaultVersionId')
        policy_doc = self._iam_client.get_policy_version(PolicyArn=arn, VersionId=policy_version).get(
            'PolicyVersion').get(
            'Document')
        self._logger.debug("Current S3 Bucket Policy")
        self._logger.debug(policy_doc)

        # update the policy to enable PutBucketPolicy on the bucket
        resources = policy_doc.get('Statement')[0].get('Resource')

        # check that the bucket isn't already in the list
        bucket_arn = "arn:aws:s3:::%s" % s3_bucket
        if bucket_arn not in resources:
            resources.append(bucket_arn)
            policy_doc.get('Statement')[0]['Resource'] = resources

            self._logger.debug("Updated S3 Bucket Policy")
            self._logger.debug(policy_doc)

            self._iam_client.create_policy_version(PolicyArn=arn, PolicyDocument=json.dumps(policy_doc),
                                                   SetAsDefault=True)
        else:
            self._logger.info("No Action Required. Bucket access already enabled.")

    def grant_datamesh_access_to_s3(self, s3_bucket: str, data_mesh_account_id: str):
        '''
        Grants the data mesh account access to S3 through a bucket policy grant
        :param s3_bucket:
        :param data_mesh_account_id:
        :return:
        '''
        # create a data lake location
        lf_client = boto3.client('lakeformation', region_name=self._current_region)
        s3_arn = "arn:aws:s3:::%s" % s3_bucket
        lf_client.register_resource(
            ResourceArn=s3_arn,
            UseServiceLinkedRole=True
        )

        # add a data lake permission for the mesh account
        lf_client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': data_mesh_account_id
            },
            Resource={
                'DataLocation': {
                    'ResourceArn': s3_arn
                },
            },
            Permissions=['DATA_LOCATION_ACCESS'],
            PermissionsWithGrantOption=['DATA_LOCATION_ACCESS']
        )

        # TODO remove bucket access based grants and update documentation based on successful test
        s3_client = boto3.client('s3')
        get_bucket_policy_response = None
        try:
            get_bucket_policy_response = s3_client.get_bucket_policy(Bucket=s3_bucket,
                                                                     ExpectedBucketOwner=self._data_producer_account_id)
        except s3_client.exceptions.from_code('NoSuchBucketPolicy'):
            pass

        # need to grant bucket access to the producer admin role
        data_mesh_producer_role_arn = utils.get_datamesh_producer_role_arn(data_mesh_account_id)
        self._logger.info("Will grant S3 access to %s" % data_mesh_producer_role_arn)

        # generate a new statement for the target bucket policy
        statement_sid = "ReadOnly-%s-%s" % (s3_bucket, data_mesh_producer_role_arn)
        conf = {
            "data_mesh_producer_role_arn": data_mesh_producer_role_arn,
            "bucket": s3_bucket, "sid": statement_sid
        }
        statement = json.loads(utils.generate_policy(template_file="producer_bucket_policy.pystache", config=conf))

        if get_bucket_policy_response is None or get_bucket_policy_response.get('Policy') is None:
            self._logger.info("Will create new S3 Bucket Policy")

            bucket_policy = {
                "Id": "Policy%s" % shortuuid.uuid(),
                "Version": "2012-10-17",
                "Statement": [
                    statement
                ]
            }

            self._logger.debug("New Bucket Policy")
            self._logger.debug(bucket_policy)

            s3_client.put_bucket_policy(
                Bucket=s3_bucket,
                ConfirmRemoveSelfBucketAccess=False,
                Policy=json.dumps(bucket_policy),
                ExpectedBucketOwner=self._data_producer_account_id
            )
        else:
            self._logger.info("Will update existing Bucket Policy")

            bucket_policy = json.loads(get_bucket_policy_response.get('Policy'))
            sid_exists = False
            for s in bucket_policy.get('Statement'):
                if s.get('Sid') == statement_sid:
                    sid_exists = True

            if sid_exists is False:
                self._logger.info("Adding new Statement for s3 Access")

                # add a statement that allows the data mesh admin producer read-only access
                bucket_policy.get('Statement').append(statement)

                self._logger.debug("New S3 Bucket Policy")
                self._logger.debug(bucket_policy)

                s3_client.put_bucket_policy(
                    Bucket=s3_bucket,
                    ConfirmRemoveSelfBucketAccess=False,
                    Policy=json.dumps(bucket_policy),
                    ExpectedBucketOwner=self._data_producer_account_id
                )
            else:
                self._logger.info("No Grant Required. Statement already exists in Policy")

    def _cleanup_table_def(self, table_def: dict):
        t = table_def.copy()

        def rm(prop):
            try:
                del t[prop]
            except KeyError:
                pass

        # remove properties from a TableInfo object returned from get_table to be compatible with put_table
        rm('DatabaseName')
        rm('CreateTime')
        rm('UpdateTime')
        rm('CreatedBy')
        rm('IsRegisteredWithLakeFormation')
        rm('CatalogId')

        return t

    def _create_mesh_table(self, table_def: dict, data_mesh_glue_client, data_mesh_lf_client, producer_ram_client,
                           producer_glue_client, data_mesh_database_name: str, producer_account_id: str,
                           data_mesh_account_id: str):
        '''
        API to create a table as a data product in the data mesh
        :param table_def:
        :param data_mesh_glue_client:
        :param data_mesh_lf_client:
        :param producer_ram_client:
        :param producer_glue_client:
        :param data_mesh_database_name:
        :param producer_account_id:
        :param data_mesh_account_id:
        :return:
        '''
        # cleanup the TableInfo object to be usable as a TableInput
        t = self._cleanup_table_def(table_def)

        self._logger.debug("Existing Table Definition")
        self._logger.debug(t)

        table_name = t.get('Name')

        # create the glue catalog entry
        try:
            data_mesh_glue_client.create_table(
                DatabaseName=data_mesh_database_name,
                TableInput=t
            )
            self._logger.info(f"Created new Glue Table {table_name}")
        except data_mesh_glue_client.exceptions.from_code('AlreadyExistsException'):
            self._logger.info(f"Glue Table {table_name} Already Exists")

        # grant access to the producer account
        perms = ['ALL']
        created_object = utils.lf_grant_permissions(
            data_mesh_account_id=self._data_mesh_account_id,
            lf_client=data_mesh_lf_client, principal=producer_account_id,
            database_name=data_mesh_database_name, table_name=table_name,
            permissions=perms,
            grantable_permissions=perms, logger=self._logger
        )

        # grant the DataMeshAdminConsumerRole rights to describe the table, meaning any consumer can see metadata
        utils.lf_grant_permissions(
            data_mesh_account_id=self._data_mesh_account_id,
            lf_client=data_mesh_lf_client, principal=utils.get_datamesh_consumer_role_arn(
                account_id=self._data_mesh_account_id),
            database_name=data_mesh_database_name, table_name=table_name,
            permissions=['DESCRIBE'],
            grantable_permissions=None, logger=self._logger
        )

        # in the producer account, accept the RAM share after 1 second - seems to be an async delay
        if created_object is not None:
            time.sleep(1)
            utils.accept_pending_lf_resource_shares(ram_client=producer_ram_client, sender_account=data_mesh_account_id,
                                                    logger=self._logger)

            # create a resource link for the data mesh table in producer account
            link_table_name = "%s_link" % table_name
            try:
                producer_glue_client.create_table(
                    DatabaseName=data_mesh_database_name,
                    TableInput={"Name": link_table_name,
                                "TargetTable": {"CatalogId": data_mesh_account_id,
                                                "DatabaseName": data_mesh_database_name,
                                                "Name": table_name
                                                }
                                }
                )
                self._logger.info(f"Created Resource Link Table {link_table_name}")
            except producer_glue_client.exceptions.from_code('AlreadyExistsException'):
                self._logger.info(f"Resource Link Table {link_table_name} Already Exists")

            return table_name, link_table_name

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

        def _no_data():
            raise Exception("Unable to find any Tables matching %s in Database %s" % (table_name_regex,
                                                                                      source_db_name))

        while finished_reading is False:
            if last_token is not None:
                get_tables_args['NextToken'] = last_token

            try:
                get_table_response = glue_client.get_tables(
                    **get_tables_args
                )
            except glue_client.EntityNotFoundException:
                _no_data()

            if 'NextToken' in get_table_response:
                last_token = get_table_response.get('NextToken')
            else:
                finished_reading = True

            # add the tables returned from this instance of the request
            if not get_table_response.get('TableList'):
                _no_data()
            else:
                all_tables.extend(get_table_response.get('TableList'))

        self._logger.info("Loaded %s tables matching description from Glue" % len(all_tables))
        return all_tables

    def _make_database_name(self, database_name: str):
        return "%s-%s" % (database_name, self._current_account.get('Account'))

    def create_data_products(self, source_database_name: str,
                             table_name_regex: str = None, sync_mesh_catalog_schedule: str = None,
                             sync_mesh_crawler_role_arn: str = None):
        # generate the target database name for the mesh
        data_mesh_database_name = self._make_database_name(source_database_name)

        # create clients for the current account and with the new credentials in the data mesh account
        producer_glue_client = self._session.client('glue', region_name=self._current_region)
        producer_ram_client = self._session.client('ram', region_name=self._current_region)
        data_mesh_glue_client = utils.generate_client(service='glue', region=self._current_region,
                                                      credentials=self._data_mesh_sts_session.get('Credentials'))
        data_mesh_lf_client = utils.generate_client(service='lakeformation', region=self._current_region,
                                                    credentials=self._data_mesh_sts_session.get('Credentials'))

        current_account = self._session.client('sts').get_caller_identity()

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
        )
        self._logger.info("Validated Data Mesh Database %s" % data_mesh_database_name)

        # grant the producer permissions to create tables on this database
        utils.lf_grant_permissions(
            data_mesh_account_id=self._data_mesh_account_id, lf_client=data_mesh_lf_client,
            principal=current_account.get('Account'),
            database_name=data_mesh_database_name, permissions=['CREATE_TABLE', 'DESCRIBE'],
            grantable_permissions=None, logger=self._logger
        )
        self._logger.info("Granted access on Database %s to Producer" % data_mesh_database_name)

        # get or create a data mesh shared database in the producer account
        utils.get_or_create_database(
            glue_client=producer_glue_client,
            database_name=data_mesh_database_name,
            database_desc="Database to contain objects objects shared with the Data Mesh Account",
        )
        self._logger.info("Validated Producer Account Database %s" % data_mesh_database_name)

        for table in all_tables:
            table_s3_path = table.get('StorageDescriptor').get('Location')

            # create a mesh table for the local copy
            created_table = self._create_mesh_table(
                table_def=table,
                data_mesh_glue_client=data_mesh_glue_client,
                data_mesh_lf_client=data_mesh_lf_client,
                producer_ram_client=producer_ram_client,
                producer_glue_client=producer_glue_client,
                data_mesh_database_name=data_mesh_database_name,
                producer_account_id=current_account.get('Account'),
                data_mesh_account_id=self._data_mesh_account_id
            )

            if sync_mesh_catalog_schedule is not None:
                glue_crawler = utils.create_crawler(
                    glue_client=producer_glue_client,
                    database_name=data_mesh_database_name,
                    table_name=created_table,
                    s3_location=table_s3_path,
                    crawler_role=sync_mesh_crawler_role_arn,
                    sync_schedule=sync_mesh_catalog_schedule
                )
                self._logger.info("Created new Glue Crawler %s" % glue_crawler)

    def get_data_product(self, database_name: str, table_name_regex: str):
        # generate a new glue client for the data mesh account
        data_mesh_glue_client = utils.generate_client('glue', region=self._current_region,
                                                      credentials=self._data_mesh_sts_session.get('Credentials'))
        # grab the tables that match the regex
        all_tables = self._load_glue_tables(
            glue_client=data_mesh_glue_client,
            catalog_id=self._data_mesh_account_id,
            source_db_name=self._make_database_name(database_name),
            table_name_regex=table_name_regex
        )
        response = []
        for t in all_tables:
            response.append({"Database": t.get('DatabaseName'), "TableName": t.get('Name'),
                             "Location": t.get('StorageDescriptor').get("Location")})

        return response

    def list_pending_access_requests(self):
        '''
        Lists all access requests that have been made by potential consumers. Pending requests can be approved or denied
        with close_access_request()
        :return:
        '''
        me = current_account = self._sts_client.get_caller_identity().get('Account')
        return self._subscription_tracker.list_subscriptions(owner_id=me, request_status=STATUS_PENDING)

    def approve_access_request(self, request_id: str,
                               grant_permissions: list = None,
                               grantable_permissions: list = None,
                               decision_notes: str = None):
        '''
        API to close an access request as approved. Approvals must be accompanied by the
        permissions to grant to the specified principal.
        :param request_id:
        :param grant_permissions:
        :param decision_notes:
        :return:
        '''
        # load the subscription
        subscription = self._subscription_tracker.get_subscription(subscription_id=request_id)

        # approver can override the requested grants
        if grant_permissions is None:
            set_permissions = subscription.get(REQUESTED_GRANTS)
        else:
            set_permissions = grant_permissions

        # grant the approved permissions in lake formation
        data_mesh_lf_client = utils.generate_client(service='lakeformation', region=self._current_region,
                                                    credentials=self._data_mesh_sts_session.get('Credentials'))
        tables = subscription.get(TABLE_NAME)
        ram_shares = {}
        for t in tables:
            # grant describe on the database
            utils.lf_grant_permissions(
                data_mesh_account_id=self._data_mesh_account_id,
                lf_client=data_mesh_lf_client,
                principal=subscription.get(SUBSCRIBER_PRINCIPAL),
                database_name=subscription.get(DATABASE_NAME),
                permissions=['DESCRIBE'],
                grantable_permissions=None, logger=self._logger
            )

            # grant validated permissions to object
            utils.lf_grant_permissions(
                data_mesh_account_id=self._data_mesh_account_id,
                lf_client=data_mesh_lf_client,
                principal=subscription.get(SUBSCRIBER_PRINCIPAL),
                database_name=subscription.get(DATABASE_NAME),
                table_name=t,
                permissions=set_permissions,
                grantable_permissions=grantable_permissions, logger=self._logger
            )

            # get the permission for the object
            perm = data_mesh_lf_client.list_permissions(
                CatalogId=self._data_mesh_account_id,
                ResourceType='TABLE',
                Resource={
                    'Table': {
                        'CatalogId': self._data_mesh_account_id,
                        'DatabaseName': subscription.get(DATABASE_NAME),
                        'Name': t
                    }
                }
            )

            if perm is not None:
                for p in perm.get('PrincipalResourcePermissions'):
                    if p.get('Principal').get('DataLakePrincipalIdentifier') == subscription.get(
                            SUBSCRIBER_PRINCIPAL) and 'DESCRIBE' in p.get(
                        'Permissions'):
                        ram_shares[t] = p.get('AdditionalDetails').get('ResourceShare')[0]
            else:
                raise Exception("Unable to Load RAM Share for Permission")

        # update the subscription to reflect the changes
        return self._subscription_tracker.update_status(
            subscription_id=request_id, status=STATUS_ACTIVE,
            permitted_grants=grant_permissions, notes=decision_notes, ram_shares=ram_shares
        )

    def deny_access_request(self, request_id: str,
                            decision_notes: str = None):
        '''
        API to close an access request as denied. The reason for the denial should be included in decision_notes.
        :param request_id:
        :param decision_notes:
        :return:
        '''
        return self._subscription_tracker.update_status(
            subscription_id=request_id, status=STATUS_DENIED,
            notes=decision_notes
        )

    def update_subscription_permissions(self, subscription_id: str, grant_permissions: list, notes: str):
        '''
        Update the permissions on a subscription
        :param subscription_id:
        :param grant_permissions:
        :param notes:
        :return:
        '''
        return self._subscription_tracker.update_grants(
            subscription_id=subscription_id, permitted_grants=grant_permissions,
            notes=notes
        )

    def delete_subscription(self, subscription_id: str, reason: str):
        '''
        Soft delete a subscription
        :param subscription_id:
        :param reason:
        :return:
        '''
        return self._subscription_tracker.delete_subscription(subscription_id=subscription_id, reason=reason)
