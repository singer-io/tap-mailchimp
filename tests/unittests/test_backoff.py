from parameterized import parameterized
import unittest
from unittest import mock
from tap_mailchimp.client import MailchimpClient, Server5xxError, ConnectionError, MailchimpRateLimitError, Timeout

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
            ['429_error_backoff', MailchimpRateLimitError, 6],
            ['Connection_error_backoff', ConnectionError,  6],
            ['Server5xx_error_backoff', Server5xxError, 6],
            ['Timeout_backoff', Timeout, 6]
    ])

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request")
    def test_get_backoff(self, name, test_exception, count, mocked_request, mocked_sleep):
        """Test case to verify backoff for 'get' works as expected"""

        mocked_request.side_effect = test_exception('exception')
        with self.assertRaises(test_exception) as e:
            response_json = self.mailchimp_client.get(self.path)
        self.assertEqual(mocked_request.call_count, count)

    @parameterized.expand([
            ['429_error_backoff', MailchimpRateLimitError, 6],
            ['Connection_error_backoff', ConnectionError,  6],
            ['Server5xx_error_backoff', Server5xxError, 6],
            ['Timeout_backoff', Timeout, 6]
    ])
    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request")
    def test_post_backoff(self, name, test_exception, count, mocked_request, mocked_sleep):
        """Test case to verify backoff for 'post' works as expected"""

        mocked_request.side_effect = test_exception('exception')
        with self.assertRaises(test_exception) as e:
            response_json = self.mailchimp_client.post(self.path)
        self.assertEqual(mocked_request.call_count, count)