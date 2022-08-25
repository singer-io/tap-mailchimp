from parameterized import parameterized
import unittest
from unittest import mock
import requests
from tap_mailchimp.client import MailchimpClient, Server5xxError, ConnectionError, MailchimpRateLimitError

class TestBackoff(unittest.TestCase):
    """
    Test cases to verify we backoff 6 times for ConnectionError, 5XX errors, 429 error
    """
    config = {
                "access_token": "test_access_token",
            }

    mailchimp_client = MailchimpClient(config)
    method = 'GET'
    path = 'path'
    url = 'url'

    @parameterized.expand([
            ['429_error_backoff', MailchimpRateLimitError, None],
            ['Connection_error_backoff', ConnectionError, None],
            ['Server5xx_error_backoff', Server5xxError, None],
    ])

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request")
    def test_backoff(self, name, test_exception, data, mocked_request, mocked_sleep):
        """Test case to verify backoff works as expected"""

        mocked_request.side_effect = test_exception('exception')
        with self.assertRaises(test_exception) as e:
            response_json = self.mailchimp_client.request(self.method, self.path, self.url)

        self.assertEqual(mocked_request.call_count, 6)
