from tap_mailchimp.client import MailchimpClient
import unittest
from unittest.mock import patch
from requests.exceptions import Timeout

REQUEST_TIMEOUT_INT = 300
REQUEST_TIMEOUT_STR = "300"
REQUEST_TIMEOUT_FLOAT = 300.0

@patch("time.sleep")
@patch("requests.Session.request", side_effect=Timeout)
class TestRequestTimeouts(unittest.TestCase):
    
    def test_no_request_timeout_in_config(self, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is not provided in config then default value is used
        """
        client = MailchimpClient({'access_token': 'as'})
        try:
            client.request('GET', "http://test", "base_url")
        except Timeout:
            pass
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)
        
    def test_integer_request_timeout_in_config(self, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config(integer value) then it should be use
        """
        client = MailchimpClient({'access_token': 'as', "request_timeout": REQUEST_TIMEOUT_INT}) # integer timeout in config
        try:
            client.request('GET', "http://test", "base_url")
        except Timeout:
            pass
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

    def test_float_request_timeout_in_config(self, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config(float value) then it should be use
        """
        client = MailchimpClient({'access_token': 'as', "request_timeout": REQUEST_TIMEOUT_FLOAT}) # float timeout in config
        try:
            client.request('GET', "http://test", "base_url")
        except Timeout:
            pass
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

    def test_string_request_timeout_in_config(self, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config(string value) then it should be use
        """
        client = MailchimpClient({'access_token': 'as', "request_timeout": REQUEST_TIMEOUT_STR}) # string timeout in config
        try:
            client.request('GET', "http://test", "base_url")
        except Timeout:
            pass
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

    def test_empty_string_request_timeout_in_config(self, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config with empty string then default value is used
        """
        client = MailchimpClient({'access_token': 'as', "request_timeout": ""}) # empty string timeout in config
        try:
            client.request('GET', "http://test", "base_url")
        except Timeout:
            pass
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)
        
    def test_zero_request_timeout_in_config(self, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config with zero value then default value is used
        """
        client = MailchimpClient({'access_token': 'as', "request_timeout": 0}) # zero value in config
        try:
            client.request('GET', "http://test", "base_url")
        except Timeout:
            pass
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

    def test_zero_string_request_timeout_in_config(self, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config with zero in string format then default value is used
        """
        client = MailchimpClient({'access_token': 'as', "request_timeout": "0"}) # zero value in config
        try:
            client.request('GET', "http://test", "base_url")
        except Timeout:
            pass
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)