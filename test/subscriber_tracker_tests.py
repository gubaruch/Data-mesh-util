import warnings
import unittest
import os
import boto3
from data_mesh_util.lib.SubscriberTracker import *

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

MESH_ACCOUNT = '887210671223'
CONSUMER_ACCOUNT = '206160724517'
PRODUCER_ACCOUNT = '600214582022'


class SubscriberTrackerTests(unittest.TestCase):
    _log_level = "DEBUG"
    _current_region = os.getenv('AWS_REGION')
    _subscription_tracker = SubscriberTracker(credentials=boto3.session.Session().get_credentials(),
                                              data_mesh_account_id=MESH_ACCOUNT,
                                              region_name=_current_region,
                                              log_level=_log_level)

    def test_subscription_lifecycle(self):
        # create a subscription
        set_grants = ['DESCRIBE', 'SELECT']
        subscription_id = self._subscription_tracker.create_subscription_request(
            owner_account_id=PRODUCER_ACCOUNT,
            database_name="tpcds",
            tables=["customer"],
            principal=CONSUMER_ACCOUNT,
            request_grants=set_grants,
            suppress_object_validation=True
        ).get("SubscriptionId")

        # status should be Pending
        sub = self._subscription_tracker.get_subscription(subscription_id=subscription_id)
        self.assertEqual(sub.get(STATUS), STATUS_PENDING)

        # now update the status to approved
        self._subscription_tracker.update_status(subscription_id=subscription_id, status=STATUS_ACTIVE, notes='OK')

        # status should be Pending, notes should match, and permissions should be as requested
        sub = self._subscription_tracker.get_subscription(subscription_id=subscription_id)
        self.assertEqual(sub.get(STATUS), STATUS_ACTIVE)
        self.assertEqual(sub.get(NOTES), {'OK'})
        self.assertListEqual(sub.get(PERMITTED_GRANTS), set_grants)

        # add some more grants
        new_grants = ['SELECT', 'DESCRIBE', 'INSERT']
        self._subscription_tracker.update_grants(subscription_id=subscription_id, permitted_grants=new_grants,
                                                 notes="Yolo")
        sub = self._subscription_tracker.get_subscription(subscription_id=subscription_id)
        self.assertListEqual(sub.get(PERMITTED_GRANTS), new_grants)

        # now retire the subscription
        self._subscription_tracker.delete_subscription(subscription_id=subscription_id, reason="No Longer Needed")
        sub = self._subscription_tracker.get_subscription(subscription_id=subscription_id, force=True)
        self.assertEqual(sub.get(STATUS), STATUS_DELETED)

    def test_list_subscriptions(self):
        # create 10 dummy subscriptions
        c = 10
        subs = []
        for i in range(0, c):
            subs.append(self._subscription_tracker.create_subscription_request(
                owner_account_id=MESH_ACCOUNT,
                database_name="Test",
                tables=["test%s" % i],
                principal=CONSUMER_ACCOUNT,
                request_grants=['DESCRIBE'],
                suppress_object_validation=True
            )[0].get("SubscriptionId")
                        )

        self.assertEqual(c, len(subs))

        # use the list api to make sure we can get 10 back using various scenarios
        def _list_subs(args):
            listed_subs = self._subscription_tracker.list_subscriptions(**args).get("Subscriptions")
            self.assertGreaterEqual(len(listed_subs), c)

        # Walk the tree of args
        _list_subs({"owner_id": MESH_ACCOUNT})
        _list_subs({"owner_id": MESH_ACCOUNT, "principal_id": CONSUMER_ACCOUNT})
        _list_subs({"owner_id": MESH_ACCOUNT, "principal_id": CONSUMER_ACCOUNT, "database_name": "Test"})
        _list_subs(
            {"owner_id": MESH_ACCOUNT, "principal_id": CONSUMER_ACCOUNT, "database_name": "Test", "tables": ["Test3"]})
        _list_subs(
            {"owner_id": MESH_ACCOUNT, "principal_id": CONSUMER_ACCOUNT, "database_name": "Test", "tables": ["Test3"],
             "includes_grants": ["DESCRIBE"]})
        _list_subs(
            {"owner_id": MESH_ACCOUNT, "principal_id": CONSUMER_ACCOUNT, "database_name": "Test", "tables": ["Test3"],
             "includes_grants": ["DESCRIBE"], "request_status": "PENDING"})

        # tests just for those conditions where we have indexes
        _list_subs({"principal_id": CONSUMER_ACCOUNT})
        _list_subs({"owner_id": MESH_ACCOUNT, "request_status": "Pending"})

        # delete the subscriptions
        ddb = boto3.resource('dynamodb', region_name=self._current_region)
        table = ddb.Table('AwsDataMeshSubscriptions')
        for s in subs:
            table.delete_item(Key={"SubscriptionId": s})
