import datetime
import time
import boto3
import os
import sys
import json

import botocore.session
import shortuuid
import logging

from data_mesh_util.lib.ApiAutomator import ApiAutomator

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
    _log_level = None
    _logger = logging.getLogger("DataMeshProducer")
    stream_handler = logging.StreamHandler(sys.stdout)
    _logger.addHandler(stream_handler)
    _data_mesh_account_id = None
    _data_producer_role_arn = None
    _data_mesh_sts_session = None
    _data_mesh_boto_session = None
    _subscription_tracker = None
    _current_account = None
    _producer_automator = None
    _mesh_automator = None

    def __init__(self, data_mesh_account_id: str, region_name: str, log_level: str = "INFO", use_credentials=None):
        self._data_mesh_account_id = data_mesh_account_id

        if region_name is None:
            raise Exception("Cannot initialize a Data Mesh Producer without an AWS Region")
        else:
            self._current_region = region_name

        if use_credentials is not None:
            self._session = utils.create_session(credentials=use_credentials, region=self._current_region)
        else:
            self._session = boto3.session.Session(region_name=self._current_region)

        self._iam_client = self._session.client('iam')
        self._sts_client = self._session.client('sts')

        self._log_level = log_level
        self._logger.setLevel(log_level)
        self._producer_automator = ApiAutomator(session=self._session, log_level=self._log_level)

        self._current_account = self._sts_client.get_caller_identity()
        session_name = utils.make_iam_session_name(self._current_account)
        self._data_producer_role_arn = utils.get_datamesh_producer_role_arn(account_id=data_mesh_account_id)
        self._data_mesh_sts_session = self._sts_client.assume_role(RoleArn=self._data_producer_role_arn,
                                                                   RoleSessionName=session_name)
        self._data_mesh_boto_session = utils.create_session(credentials=self._data_mesh_sts_session.get('Credentials'),
                                                            region=self._current_region)
        self._mesh_automator = ApiAutomator(session=self._data_mesh_boto_session, log_level=self._log_level)

        self._logger.debug("Created new STS Session for Data Mesh Admin Producer")
        self._logger.debug(self._data_mesh_sts_session)

        # validate that we are running in the data mesh account
        utils.validate_correct_account(self._data_mesh_sts_session.get('Credentials'), self._data_mesh_account_id)

        self._subscription_tracker = SubscriberTracker(credentials=self._data_mesh_sts_session.get('Credentials'),
                                                       data_mesh_account_id=data_mesh_account_id,
                                                       region_name=self._current_region,
                                                       log_level=log_level)

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
        created_object = self._mesh_automator.lf_grant_permissions(
            data_mesh_account_id=self._data_mesh_account_id,
            principal=producer_account_id,
            database_name=data_mesh_database_name, table_name=table_name,
            permissions=perms,
            grantable_permissions=perms
        )

        # grant the DataMeshAdminConsumerRole rights to describe the table, meaning any consumer can see metadata
        self._mesh_automator.lf_grant_permissions(
            data_mesh_account_id=self._data_mesh_account_id,
            principal=utils.get_datamesh_consumer_role_arn(
                account_id=self._data_mesh_account_id),
            database_name=data_mesh_database_name, table_name=table_name,
            permissions=['DESCRIBE'],
            grantable_permissions=None
        )

        # in the producer account, accept the RAM share after 1 second - seems to be an async delay
        if created_object is not None:
            time.sleep(1)
            self._producer_automator.accept_pending_lf_resource_shares(
                sender_account=data_mesh_account_id
            )

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

        self._logger.info(f"Loaded {len(all_tables)} tables matching {table_name_regex} from Glue")
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
        self._mesh_automator.get_or_create_database(
            database_name=data_mesh_database_name,
            database_desc="Database to contain objects from Source Database %s.%s" % (
                current_account.get('Account'), source_database_name)
        )
        self._logger.info("Validated Data Mesh Database %s" % data_mesh_database_name)

        # set default permissions on db
        self._mesh_automator.configure_db_permissions(database_name=data_mesh_database_name)

        # grant the producer permissions to create tables on this database
        self._mesh_automator.lf_grant_permissions(
            data_mesh_account_id=self._data_mesh_account_id,
            principal=current_account.get('Account'),
            database_name=data_mesh_database_name, permissions=['CREATE_TABLE', 'DESCRIBE'],
            grantable_permissions=None
        )
        self._logger.info("Granted access on Database %s to Producer" % data_mesh_database_name)

        # get or create a data mesh shared database in the producer account
        self._producer_automator.get_or_create_database(
            database_name=data_mesh_database_name,
            database_desc="Database to contain objects objects shared with the Data Mesh Account",
        )
        self._logger.info("Validated Producer Account Database %s" % data_mesh_database_name)

        for table in all_tables:
            table_s3_path = table.get('StorageDescriptor').get('Location')

            # create a data lake location for the s3 path
            data_mesh_lf_client.register_resource(
                ResourceArn=table_s3_path,
                UseServiceLinkedRole=True,
                RoleArn=self._data_producer_role_arn
            )

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

            # add a bucket policy entry allowing the data mesh lakeformation service linked role to perform GetObject*
            table_bucket = table_s3_path.split("/")[2]
            self._producer_automator.add_bucket_policy_entry(
                principal_account=self._data_mesh_account_id,
                access_path=table_bucket
            )

            if sync_mesh_catalog_schedule is not None:
                glue_crawler = self._producer_automator.create_crawler(
                    database_name=data_mesh_database_name,
                    table_name=created_table,
                    s3_location=table_s3_path,
                    crawler_role=sync_mesh_crawler_role_arn,
                    sync_schedule=sync_mesh_catalog_schedule
                )

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
        me = self._sts_client.get_caller_identity().get('Account')
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
            # get the data location for the table
            data_mesh_glue_client = utils.generate_client(service='glue', region=self._current_region,
                                                          credentials=self._data_mesh_sts_session.get('Credentials'))
            table = data_mesh_glue_client.get_table(DatabaseName=subscription.get(DATABASE_NAME), Name=t)
            table_s3_path = table.get('Table').get('StorageDescriptor').get('Location')

            # add a bucket policy entry allowing the consumer lakeformation service linked role to perform GetObject*
            table_bucket = table_s3_path.split("/")[2]
            self._producer_automator.add_bucket_policy_entry(
                principal_account=subscription.get(SUBSCRIBER_PRINCIPAL),
                access_path=table_bucket
            )

            # grant describe on the database
            self._mesh_automator.lf_grant_permissions(
                data_mesh_account_id=self._data_mesh_account_id,
                principal=subscription.get(SUBSCRIBER_PRINCIPAL),
                database_name=subscription.get(DATABASE_NAME),
                permissions=['DESCRIBE'],
                grantable_permissions=None
            )

            # grant validated permissions to object
            self._mesh_automator.lf_grant_permissions(
                data_mesh_account_id=self._data_mesh_account_id,
                principal=subscription.get(SUBSCRIBER_PRINCIPAL),
                database_name=subscription.get(DATABASE_NAME),
                table_name=t,
                permissions=set_permissions,
                grantable_permissions=grantable_permissions
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
