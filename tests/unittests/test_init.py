import unittest
from unittest import mock
from parameterized import parameterized

from tap_mailchimp import do_discover, main
from tap_mailchimp.client import MailchimpClient
from test_streams import Catalog

class MockParseArgs:
    discover = {}
    catalog = {}
    config = {}
    state = {}
    def __init__(self, discover=False, catalog=False):
        self.discover = discover
        self.catalog = catalog

class TestInit(unittest.TestCase):
    """Test cases to verify the working of __init__ file"""

    client = MailchimpClient(config={})

    def test_do_discover_authentication_error(self):
        """Test case to verify we raise an exception when we get the error during authentication"""

        with self.assertRaises(Exception) as e:
            do_discover(client=self.client)

        self.assertEqual(str(e.exception), "Error testing Mailchimp authentication")

    @mock.patch("json.dump")
    @mock.patch("tap_mailchimp.discover", return_value=Catalog(stream_name="test_stream"))
    @mock.patch("tap_mailchimp.client.MailchimpClient.get")
    def test_do_discover_catalog_generation(self, mocked_get, mocked_discover, mocked_json_dump):
        """Test case to verify we generate catalog on successful authentication"""

        do_discover(client=self.client)

        args, kwargs = mocked_json_dump.call_args
        self.assertEqual(args[0], {"field": "value"})

    @parameterized.expand([
        ["discovery", [True, False], [1, 0]],
        ["sync", [False, True], [0, 1]],
        ["no_discovery_no_sync", [False, False], [0, 0]]
    ])
    @mock.patch("singer.utils.parse_args")
    @mock.patch("tap_mailchimp.do_discover")
    @mock.patch("tap_mailchimp.sync")
    def test_main(self, name, test_data, expected_data, mocked_sync, mocked_do_discover, mocked_parse_args):
        """Test case to verify we call discovery and sync as per the passed args"""
        mocked_parse_args.return_value = MockParseArgs(discover=test_data[0], catalog=test_data[1])

        main()

        self.assertEqual(mocked_do_discover.call_count, expected_data[0])
        self.assertEqual(mocked_sync.call_count, expected_data[1])
