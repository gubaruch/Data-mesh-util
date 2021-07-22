from constants import *
import boto3


class SubscriberTracker:
    _dynamo_client = None
    _table_info = None

    def __init__(self, region_name: str):
        self._dynamo_client = boto3.client('dynamodb', region_name=region_name)

        self._table_info = self._init_table()

    def _init_table(self):
        def _extract_format(t):
            return {
                'Table': t.get('TableArn'),
                'Stream': t.get('LatestStreamArn')
            }

        try:
            response = self._dynamo_client.create_table(
                TableName=SUBSCRIPTIONS_TRACKER_TABLE,
                AttributeDefinitions=[
                    {
                        'AttributeName': 'SubscriptionId',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'SubscriberPrincipal',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'OwnerPrincipal',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'Status',
                        'AttributeType': 'S'
                    }
                ],
                KeySchema=[
                    {
                        'AttributeName': 'SubscriptionId',
                        'KeyType': 'HASH'
                    }
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': "%s-%s" % (SUBSCRIPTIONS_TRACKER_TABLE, 'Owner'),
                        'KeySchema': [
                            {
                                'AttributeName': 'OwnerPrincipal',
                                'KeyType': 'HASH',
                            },
                            {
                                'AttributeName': 'Status',
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
                                'AttributeName': 'SubscriberPrincipal',
                                'KeyType': 'HASH',
                            },
                            {
                                'AttributeName': 'Status',
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

            return _extract_format(response.get('TableDescription'))
        except (self._dynamo_client.exceptions.from_code('AlreadyExistsException'),
                self._dynamo_client.exceptions.from_code('ResourceInUseException')):
            response = self._dynamo_client.describe_table(
                TableName=SUBSCRIPTIONS_TRACKER_TABLE
            )

            return _extract_format(response.get('Table'))

    def get_endpoints(self):
        return self._table_info
