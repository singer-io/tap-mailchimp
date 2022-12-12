import unittest
from unittest import mock
from tap_mailchimp.streams import Unsubscribes
from tap_mailchimp.client import MailchimpClient
from parameterized import parameterized

class Schema:
    """
    Class to provide required attributes for test cases.
    """

    def __init__(self, stream_name):
        self.stream_name = stream_name

    def to_dict(self):
        return {"stream": self.stream_name}


class Catalog:
    """
    Class to provide required attributes for test cases.
    """

    def __init__(self, stream_name):
        self.stream_name = stream_name

    def get_stream(self, stream_name):
        return Streams(self.stream_name)


class Streams:
    """
    Class to provide required attributes for test cases.
    """

    def __init__(self, stream_name):
        self.stream_name = stream_name
        self.path = "/test_path"
        self.schema = Schema(stream_name)
        self.key_properties = []
        self.metadata = [{"breadcrumb": (), "metadata": {"valid-replication-keys": []}}]


class TestUnsubcribes(unittest.TestCase):

    stream_object = Unsubscribes(
            state="",
            client=MailchimpClient,
            config={},
            catalog=Catalog("unsubscribes"),
            selected_stream_names=[Streams("unsubscribes")],
            child_streams_to_sync=None,
        )
    
    @parameterized.expand([
        ['record_date_greater_then_sync_start_date', "2014-09-15T23:37:21.000000Z", "2014-09-21T23:37:21.000000Z"],
        ['record_date_less_then_sync_start_date', "2014-09-22T23:37:21.000000Z", None],
    ])
    def test_process_records(self, name, sync_start_date, expected_bookmark):
        """Test to verify sync for `unsubscribes`"""

        object_ = self.stream_object

        test_record = [
            {"campaign_id": "test_campaign", "timestamp": "2014-09-21T23:37:21.000000Z"}
        ]

        actual_bookmark = object_.process_records(
            records=test_record, max_bookmark_field=None, sync_start=sync_start_date
        )
        
        self.assertEqual(actual_bookmark, expected_bookmark)