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
    
    def test_no_request_timeout_in_config(self, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is not provided in config then default value is used
        """
        # Initialize MailchimpClient object
        client = MailchimpClient({'access_token': 'as'})
        try:
            client.request('GET', "http://test", "base_url")
        except requests.exceptions.Timeout:
            pass

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

@patch("requests.Session.request", side_effect=get_mock_http_response)
class TestRequestTimeoutsValue(unittest.TestCase):
    
    def test_no_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is not provided in config then default value is used
        """
        # Initialize MailchimpClient object
        client = MailchimpClient({'access_token': 'as'})
        
        # Call request method which call requests.Session.request with timeout
        client.request('GET', "http://test", "base_url")

        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument
        
    def test_integer_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config(integer value) then it should be use
        """
        # Initialize MailchimpClient object
        client = MailchimpClient({'access_token': 'as', "request_timeout": REQUEST_TIMEOUT_INT}) # integer timeout in config
        
        # Call request method which call requests.Session.request with timeout
        client.request('GET', "http://test", "base_url")
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

    def test_float_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config(float value) then it should be use
        """
        # Initialize MailchimpClient object
        client = MailchimpClient({'access_token': 'as', "request_timeout": REQUEST_TIMEOUT_FLOAT}) # float timeout in config
        
        # Call request method which call requests.Session.request with timeout
        client.request('GET', "http://test", "base_url")
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

    def test_string_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config(string value) then it should be use
        """
        # Initialize MailchimpClient object
        client = MailchimpClient({'access_token': 'as', "request_timeout": REQUEST_TIMEOUT_STR}) # string timeout in config
        
        # Call request method which call requests.Session.request with timeout
        client.request('GET', "http://test", "base_url")
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

    def test_empty_string_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config with empty string then default value is used
        """
        # Initialize MailchimpClient object
        client = MailchimpClient({'access_token': 'as', "request_timeout": ""}) # empty string timeout in config
        
        # Call request method which call requests.Session.request with timeout
        client.request('GET', "http://test", "base_url")
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument
        
    def test_zero_int_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config with int zero value then default value is used
        """
        # Initialize MailchimpClient object
        client = MailchimpClient({'access_token': 'as', "request_timeout": 0}) # int zero value in config
        
        # Call request method which call requests.Session.request with timeout
        client.request('GET', "http://test", "base_url")
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

    def test_zero_string_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config with string zero in string format then default value is used
        """
        client = MailchimpClient({'access_token': 'as', "request_timeout": "0"}) # string zero value in config
        
        # Call request method which call requests.Session.request with timeout
        client.request('GET', "http://test", "base_url")
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument
