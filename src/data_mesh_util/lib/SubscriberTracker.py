import logging
import time
import sys
from constants import *
from boto3.dynamodb.conditions import Attr, Or, And, Not, Key
import shortuuid
from datetime import datetime

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
    _table_info = None
    _table = None
    _logger = None

    def __init__(self, dynamo_client, dynamo_resource, log_level: str = "INFO"):
        '''
        Initialize a subscriber tracker. Requires the external creation of clients because we will span roles
        :param dynamo_client:
        :param dynamo_resource:
        :param log_level:
        '''
        self._dynamo_client = dynamo_client
        self._dynamo_resource = dynamo_resource

        self._table_info = self._init_table()

        _logger = logging.getLogger("SubscriberTracker")

        # make sure we always log to standard out
        stream_handler = logging.StreamHandler(sys.stdout)
        _logger.addHandler(stream_handler)
        _logger.setLevel(log_level)

    def _init_table(self):
        t = None

        def _extract_format():
            return {
                'Table': t.get('TableArn'),
                'Stream': t.get('LatestStreamArn')
            }

        try:
            response = self._dynamo_client.describe_table(
                TableName=SUBSCRIPTIONS_TRACKER_TABLE
            )

            t = response.get('Table')
        except self._dynamo_client.exceptions.ResourceNotFoundException:
            t = self._create_table()

        self._table = self._dynamo_resource.Table(SUBSCRIPTIONS_TRACKER_TABLE)

        return _extract_format()

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

    def create_subscription_request(self, owner_account_id: str, database_name: str, table_name: str, principal: str,
                                    request_grants: list):
        # look up if there is already a subscription request for this object
        d = Attr(DATABASE_NAME).eq(database_name)
        if table_name is not None:
            find_filter = And(d, Attr(TABLE_NAME).eq(table_name))
        else:
            find_filter = d

        found = self._table.query(
            IndexName=self.subscriber_indexname(),
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression=SUBSCRIPTION_ID,
            ConsistentRead=False,
            KeyConditionExpression=Key(SUBSCRIBER_PRINCIPAL).eq(principal),
            FilterExpression=find_filter
        )

        if found.get('Count') == 1:
            return found.get('Items')[0].get(SUBSCRIPTION_ID)
        else:
            # generate the subscription item
            item = _add_www(item={
                SUBSCRIPTION_ID: _generate_id(),
                OWNER_PRINCIPAL: owner_account_id,
                SUBSCRIBER_PRINCIPAL: principal,
                REQUESTED_GRANTS: request_grants,
                DATABASE_NAME: database_name,
                TABLE_NAME: table_name
            }, principal=principal)

            # generate the condition. We'll block any case where the subscriber principal has requested the same database and table
            cond = Not(
                And(
                    And(
                        Attr(SUBSCRIBER_PRINCIPAL).eq(principal),
                        Attr(DATABASE_NAME).eq(database_name)
                    ),
                    Attr(TABLE_NAME).eq(table_name)
                )
            )

            # put the item in
            response = self._table.put_item(
                Item=item,
                ConditionExpression=cond
            )

            return response.get('Attributes').get(SUBSCRIPTION_ID)

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
