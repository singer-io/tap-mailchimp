import unittest
from unittest import mock

from parameterized import parameterized

from tap_mailchimp.sync import get_streams_to_sync, sync


class Schema:
    """Class to provide required attributes for test cases."""

    def __init__(self, stream):
        self.stream = stream


class Stream:
    """Class to provide required attributes for test cases."""

    def __init__(self, stream_name):
        self.tap_stream_id = stream_name
        self.stream = stream_name
        self.schema = Schema(stream_name)
        self.metadata = {}


class Catalog:
    """Class to provide required attributes for test cases."""

    def __init__(self, stream):
        self.stream = stream
        self.tap_stream_id = stream
        self.schema = Schema(stream)
        self.metadata = {}

    def get_selected_streams(self, state):
        """Returns selected streams."""
        return (Catalog(i) for i in ["automations", "campaigns"])

    def get_stream(self, stream_name):
        return Stream(stream_name)


class TestSyncMode(unittest.TestCase):
    """Test class to verify working of functions in sync mode."""

    streams = [Stream("lists"), Stream("list_segments"), Stream("automations"), Stream("list_segment_members")]

    @parameterized.expand(
        [
            ["no_parent", ["lists", "automations"], [2, 0]],
            ["single_level_inheritance_parent_not_selected", ["list_segments", "automations"], [2, 0]],
            ["single_level_inheritance_parent_selected", ["list_segments", "automations", "lists"], [2, 0]],
            ["multiple_level_inheritance_parent_not_selected", ["list_segment_members", "automations"], [2, 1]],
            [
                "multiple_level_inheritance_sub_parent_selected",
                ["list_segment_members", "automations", "list_segments"],
                [2, 1],
            ],
            [
                "multiple_level_inheritance_super_parent_selected",
                ["list_segment_members", "automations", "lists"],
                [2, 1],
            ],
            [
                "multiple_level_inheritance_all_parents_selected",
                ["list_segment_members", "automations", "lists", "list_segments"],
                [2, 1],
            ],
        ]
    )
    def test_get_streams_to_sync(self, name, test_value1, test_value2):
        """Test case to verify that parent_streams and child_streams (if
        existing) are selected properly."""
        selected_streams = []

        for stream in test_value1:
            selected_streams.append(Stream(stream))

        catalog = Catalog(stream=self.streams)
        streams, child = get_streams_to_sync(
            catalog=catalog, selected_streams=selected_streams, selected_stream_names=test_value1
        )

        self.assertEqual(len(streams), test_value2[0])
        self.assertEqual(len(child), test_value2[1])

    @mock.patch("singer.set_currently_syncing")
    @mock.patch("singer.write_state")
    @mock.patch("tap_mailchimp.streams.FullTable.sync")
    @mock.patch("tap_mailchimp.streams.BaseStream.write_schema")
    @mock.patch("tap_mailchimp.client.MailchimpClient")
    def test_sync(
        self, mocked_client, mocked_write_stream, mocked_full_table_sync, mocked_write_state, mocked_currently_syncing
    ):

        catalog = Catalog(stream=self.streams)
        config = {
            "access_token": "TEST",
            "request_timeout": 300,
            "client_secret": "test",
            "start_date": "2010-01-01T00:00:00Z",
        }

        sync(client=mocked_client, catalog=catalog, state={}, config=config)

        # The functions singer.write_state and singer.set_currently_syncing are
        # called whenever a stream is synced and also at the end of sync. Here
        # we have selected 2 streams, hence the call_count for both of these functions
        # should be 3.
        self.assertEqual(mocked_write_state.call_count, 3)
        self.assertEqual(mocked_currently_syncing.call_count, 3)
