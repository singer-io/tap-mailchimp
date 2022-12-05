from tap_mailchimp.client import MailchimpClient
import unittest
from unittest.mock import patch
import requests

REQUEST_TIMEOUT_INT = 300
REQUEST_TIMEOUT_STR = "300"
REQUEST_TIMEOUT_FLOAT = 300.0

# Mock response object


def get_mock_http_response(*args, **kwargs):
    contents = '{"access_token": "test", "expires_in":100, "accounts":[{"id": 12}]}'
    response = requests.Response()
    response.status_code = 200
    response._content = contents.encode()
    return response

@patch("time.sleep")
@patch("requests.Session.request", side_effect=requests.exceptions.Timeout)
class TestRequestTimeoutsBackoff(unittest.TestCase):

    def test_request_timeout_backoff(self, mocked_request, mock_sleep):
        """
            Verify request function is backoff for 5 times on Timeout exceeption
        """
        # Initialize MailchimpClient object
        client = MailchimpClient(config={"access_token": "as"})
        try:
            client.request(
                method="GET",
                path="http://test",
                url="base_url"
            )
        except requests.exceptions.Timeout:
            pass

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

@patch("requests.Session.request", side_effect=get_mock_http_response)
class TestRequestTimeoutsValue(unittest.TestCase):

    def test_no_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is not provided in config then default value(300) is used
        """
        # Initialize MailchimpClient object
        client = MailchimpClient(config={"access_token": "as"})

        # Call request method which call requests.Session.request with timeout
        client.request(
            method="GET",
            path="http://test",
            url="base_url"
        )

        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        # Verify timeout argument
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT)

    def test_integer_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config(integer value) then it should be use
        """
        # Initialize MailchimpClient object
        # integer timeout in config
        client = MailchimpClient(
            config={"access_token": "as", "request_timeout": REQUEST_TIMEOUT_INT})

        # Call request method which call requests.Session.request with timeout
        client.request(
            method="GET",
            path="http://test",
            url="base_url"
        )

        # Verify requests.Session.request is called with expected timeout.
        # If none zero positive integer or string value passed in the config then it converted to float value. So, here we are verifying the same.
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT)

    def test_float_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config(float value) then it should be use
        """
        # Initialize MailchimpClient object
        # float timeout in config
        client = MailchimpClient(
            config={"access_token": "as", "request_timeout": REQUEST_TIMEOUT_FLOAT})

        # Call request method which call requests.Session.request with timeout
        client.request(
            method="GET",
            path="http://test",
            url="base_url"
        )

        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        # Verify timeout argument
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT)

    def test_string_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config(string value) then it should be use
        """
        # Initialize MailchimpClient object
        # string timeout in config
        client = MailchimpClient(
            config={"access_token": "as", "request_timeout": REQUEST_TIMEOUT_STR})

        # Call request method which call requests.Session.request with timeout
        client.request(
            method="GET",
            path="http://test",
            url="base_url"
        )

        # Verify requests.Session.request is called with expected timeout
        # If none zero positive integer or string value passed in the config then it converted to float value. So, here we are verifying the same.
        args, kwargs = mocked_request.call_args
        # Verify timeout argument
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT)

    def test_empty_string_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config with empty string then default value(300) is used
        """
        # Initialize MailchimpClient object
        # empty string timeout in config
        client = MailchimpClient(
            config={"access_token": "as", "request_timeout": ""})

        # Call request method which call requests.Session.request with timeout
        client.request(
            method="GET",
            path="http://test",
            url="base_url"
        )

        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        # Verify timeout argument
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT)

    def test_zero_int_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config with int zero value then default value(300) is used
        """
        # Initialize MailchimpClient object
        # int zero value in config
        client = MailchimpClient(
            config={"access_token": "as", "request_timeout": 0})

        # Call request method which call requests.Session.request with timeout
        client.request(
            method="GET",
            path="http://test",
            url="base_url"
        )

        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        # Verify timeout argument
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT)

    def test_zero_string_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config with string zero in string format then default value(300) is used
        """
        client = MailchimpClient(
            config={"access_token": "as", "request_timeout": "0"})  # string zero value in config

        # Call request method which call requests.Session.request with timeout
        client.request(
            method="GET",
            path="http://test",
            url="base_url"
        )

        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        # Verify timeout argument
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT)
