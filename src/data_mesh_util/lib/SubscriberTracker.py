from constants import *
import boto3
from boto3.dynamodb.conditions import Attr, Or, And

STATUS_ACTIVE = 'Active'
STATUS_DENIED = 'Denied'
STATUS_PENDING = 'Pending'
STATUS_DELETED = 'Deleted'
SUBSCRIPTION_ID = 'SubscriptionId'
OWNER_PRINCIPAL = 'OwnerPrincipal'
SUBSCRIBER_PRINCIPAL = 'SubscriberPrincipal'
STATUS = 'Status'


class SubscriberTracker:
    _dynamo_client = None
    _dynamo_resource = None
    _table_info = None
    _table = None

    def __init__(self, region_name: str):
        self._dynamo_client = boto3.client('dynamodb', region_name=region_name)
        self._dynamo_resource = boto3.resource('dynamodb', region_name=region_name)

        self._table_info = self._init_table()

    def _init_table(self):
        t = None

        def _extract_format():
            return {
                'Table': t.get('TableArn'),
                'Stream': t.get('LatestStreamArn')
            }

        try:
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
                        'IndexName': "%s-%s" % (SUBSCRIPTIONS_TRACKER_TABLE, 'Owner'),
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
                        'IndexName': "%s-%s" % (SUBSCRIPTIONS_TRACKER_TABLE, 'Subscriber'),
                        'KeySchema': [
                            {
                                'AttributeName': SUBSCRIBER_PRINCIPAL,
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
                    }
                ],
                BillingMode='PAY_PER_REQUEST',
                StreamSpecification={
                    'StreamEnabled': True,
                    'StreamViewType': 'NEW_AND_OLD_IMAGES'
                },
                Tags=DEFAULT_TAGS
            )

            t = response.get('TableDescription')
        except (self._dynamo_client.exceptions.from_code('AlreadyExistsException'),
                self._dynamo_client.exceptions.from_code('ResourceInUseException')):
            response = self._dynamo_client.describe_table(
                TableName=SUBSCRIPTIONS_TRACKER_TABLE
            )

            t = response.get('Table')

        self._table = self._dynamo_resource.Table(SUBSCRIPTIONS_TRACKER_TABLE)

        return _extract_format()

    def get_endpoints(self):
        return self._table_info

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
