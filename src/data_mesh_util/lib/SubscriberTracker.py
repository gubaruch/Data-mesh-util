import logging
import sys
from constants import *
from boto3.dynamodb.conditions import Attr, Or, And, Key
import shortuuid
from datetime import datetime
import data_mesh_util.lib.utils as utils

STATUS_ACTIVE = 'Active'
STATUS_DENIED = 'Denied'
STATUS_PENDING = 'Pending'
STATUS_DELETED = 'Deleted'
SUBSCRIPTION_ID = 'SubscriptionId'
OWNER_PRINCIPAL = 'OwnerPrincipal'
SUBSCRIBER_PRINCIPAL = 'SubscriberPrincipal'
STATUS = 'Status'
CREATION_DATE = 'CreationDate'
CREATED_BY = 'CreatedBy'
UPDATED_DATE = 'UpdatedDate'
UPDATED_BY = 'UpdatedBy'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
DATABASE_NAME = 'DatabaseName'
TABLE_NAME = 'TableName'
REQUESTED_GRANTS = 'RequestedGrants'


def _generate_id():
    return shortuuid.uuid()


def _format_time_now():
    return datetime.now().strftime(DATE_FORMAT)


def _add_www(item: dict, principal: str, new: bool = True):
    '''
    Method to decorate a DynamoDB item with Who What When attributes
    :param item:
    :param principal:
    :param new:
    :return:
    '''
    if new:
        item[CREATION_DATE] = _format_time_now()
        item[CREATED_BY] = principal
    else:
        item[UPDATED_DATE] = _format_time_now()
        item[UPDATED_BY] = principal

    return item


class SubscriberTracker:
    _dynamo_client = None
    _dynamo_resource = None
    _glue_client = None
    _table_info = None
    _table = None
    _logger = None
    _region = None

    def __init__(self, credentials, region_name: str, log_level: str = "INFO"):
        '''
        Initialize a subscriber tracker. Requires the external creation of clients because we will span roles
        :param dynamo_client:
        :param dynamo_resource:
        :param log_level:
        '''
        self._region = region_name
        self._dynamo_client = utils.generate_client(service='dynamodb', region=region_name,
                                                    credentials=credentials)
        self._dynamo_resource = utils.generate_resource(service='dynamodb', region=region_name,
                                                        credentials=credentials)
        self._glue_client = utils.generate_client(service='glue', region=region_name,
                                                  credentials=credentials)
        self._table_info = self._init_table()

        _logger = logging.getLogger("SubscriberTracker")

        # make sure we always log to standard out
        stream_handler = logging.StreamHandler(sys.stdout)
        _logger.addHandler(stream_handler)
        _logger.setLevel(log_level)

    def _init_table(self):
        t = None
        try:
            response = self._dynamo_client.describe_table(
                TableName=SUBSCRIPTIONS_TRACKER_TABLE
            )

            t = response.get('Table')
        except self._dynamo_client.exceptions.ResourceNotFoundException:
            t = self._create_table()

        self._table = self._dynamo_resource.Table(SUBSCRIPTIONS_TRACKER_TABLE)

        return {
            'Table': t.get('TableArn'),
            'Stream': t.get('LatestStreamArn')
        }

    def subscriber_indexname(self):
        return "%s-%s" % (SUBSCRIPTIONS_TRACKER_TABLE, 'Subscriber')

    def owner_indexname(self):
        return "%s-%s" % (SUBSCRIPTIONS_TRACKER_TABLE, 'Owner')

    def _create_table(self):
        response = self._dynamo_client.create_table(
            TableName=SUBSCRIPTIONS_TRACKER_TABLE,
            AttributeDefinitions=[
                {
                    'AttributeName': SUBSCRIPTION_ID,
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': SUBSCRIBER_PRINCIPAL,
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': OWNER_PRINCIPAL,
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': STATUS,
                    'AttributeType': 'S'
                }
            ],
            KeySchema=[
                {
                    'AttributeName': SUBSCRIPTION_ID,
                    'KeyType': 'HASH'
                }
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': self.owner_indexname(),
                    'KeySchema': [
                        {
                            'AttributeName': OWNER_PRINCIPAL,
                            'KeyType': 'HASH',
                        },
                        {
                            'AttributeName': STATUS,
                            'KeyType': 'RANGE',
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'KEYS_ONLY'
                    }
                },
                {
                    'IndexName': self.subscriber_indexname(),
                    'KeySchema': [
                        {
                            'AttributeName': SUBSCRIBER_PRINCIPAL,
                            'KeyType': 'HASH',
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'INCLUDE',
                        'NonKeyAttributes': [
                            DATABASE_NAME, TABLE_NAME
                        ]
                    }
                }
            ],
            BillingMode='PAY_PER_REQUEST',
            StreamSpecification={
                'StreamEnabled': True,
                'StreamViewType': 'NEW_AND_OLD_IMAGES'
            },
            Tags=DEFAULT_TAGS
        )

        return response.get('TableDescription')

    def get_endpoints(self):
        return self._table_info

    def _validate_object(self, database_name: str, table_name: str):
        try:
            response = self._glue_client.get_table(
                DatabaseName=database_name,
                Name=table_name
            )

            if 'Table' not in response:
                return False
            else:
                return True
        except self._glue_client.exceptions.AccessDeniedException:
            # if we get access denied here, it's because the object doesn't exist
            return False

    def create_subscription_request(self, owner_account_id: str, database_name: str, tables: list, principal: str,
                                    request_grants: list):
        # look up if there is already a subscription request for this object
        d = Attr(DATABASE_NAME).eq(database_name)

        def _sub_exists(filter):
            found = self._table.query(
                IndexName=self.subscriber_indexname(),
                Select='SPECIFIC_ATTRIBUTES',
                ProjectionExpression=SUBSCRIPTION_ID,
                ConsistentRead=False,
                KeyConditionExpression=Key(SUBSCRIBER_PRINCIPAL).eq(principal),
                FilterExpression=filter
            )

            if found.get('Count') == 1:
                return found.get('Items')[0].get(SUBSCRIPTION_ID)
            else:
                return None

        def _create_subscription(item, principal):
            # generate a new subscription
            item = _add_www(item=item, principal=principal)

            self._table.put_item(
                Item=item
            )

        subscriptions = []
        if tables is None:
            sub_id = _sub_exists(d)
            if sub_id is not None:
                sub_id = _generate_id()

            # create a database level subscription
            item = {
                SUBSCRIPTION_ID: sub_id,
                OWNER_PRINCIPAL: owner_account_id,
                SUBSCRIBER_PRINCIPAL: principal,
                REQUESTED_GRANTS: request_grants,
                DATABASE_NAME: database_name
            }
            _create_subscription(item=item, principal=principal)

            subscriptions.append({
                DATABASE_NAME: database_name,
                SUBSCRIPTION_ID: sub_id
            })
        else:
            for table_name in tables:
                # validate if the object exists
                exists = self._validate_object(database_name=database_name, table_name=table_name)

                if not exists:
                    raise Exception("Table %s does not exist in %s" % (table_name, database_name))
                else:
                    subscription_id = _sub_exists(And(d, Attr(TABLE_NAME).eq(table_name)))
                    if subscription_id is None:
                        subscription_id = _generate_id()

                    item = {
                        SUBSCRIPTION_ID: subscription_id,
                        OWNER_PRINCIPAL: owner_account_id,
                        SUBSCRIBER_PRINCIPAL: principal,
                        REQUESTED_GRANTS: request_grants,
                        DATABASE_NAME: database_name,
                        TABLE_NAME: table_name
                    }
                    _create_subscription(item=item, principal=principal)

                    subscriptions.append({TABLE_NAME: table_name, SUBSCRIPTION_ID: subscription_id})

        return subscriptions

    def update_status(self, subscription_id: str, status: str):
        '''
        Updates the status of a subscription. Valid transitions are:
        PENDING->ACTIVE
        PENDING->DENIED
        DENIED->ACTIVE
        ACTIVE->DELETED

        :param subscription_id:
        :param status:
        :return:
        '''
        # build the map of proposed status to allowed status
        if status == STATUS_ACTIVE:
            expected = Or(Attr(STATUS).eq(STATUS_PENDING), Attr(STATUS).eq(STATUS_DENIED))
        elif status == STATUS_DENIED:
            expected = Attr(STATUS).eq(STATUS_PENDING)
        elif status == STATUS_DELETED:
            expected = Attr(STATUS).eq(STATUS_ACTIVE)

        response = self._table.update_item(
            Key={
                SUBSCRIPTION_ID: subscription_id
            },
            UpdateExpression="set :status = #status",
            ExpressionAttributeNames={
                ":status": STATUS
            },
            ExpressionAttributeValues={
                "#status": status
            },
            ConditionExpression=expected
        )

        if response is None or response.get('ConsumedCapacity') is None or response.get('ConsumedCapacity').get(
                'CapacityUnits') == 0:
            raise Exception("Invalid State Transition")
        else:
            return True
