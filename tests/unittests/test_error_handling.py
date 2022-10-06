from tap_mailchimp import do_discover
from parameterized import parameterized
import unittest
from unittest import mock
import requests,json
from tap_mailchimp import client
from tap_mailchimp.client import MailchimpError, MailchimpBadRequestError, MailchimpUnAuthorizedError, MailchimpForbiddenError, MailchimpNotFoundError, MailchimpMethodNotAllowedError, MailchimpNestingError, MailchimpInvalidMethodError, MailchimpUpgradeError, MailchimpRateLimitError, MailchimpInternalServerError, Server5xxError

def get_mock_http_response(status_code, contents):
    """Returns mock rep"""
    response = requests.Response()
    response.status_code = status_code
    response._content = contents.encode()
    return response

class TestCustomErrorHandling(unittest.TestCase):
    """
    Test cases to verify we get custom error messages when we do not receive error from the API
    """

    config = {
        "access_token": "test_access_token",
    }

    mailchimp_client = client.MailchimpClient(config)
    method = 'GET'
    path = 'path'
    url = 'url'

    @parameterized.expand([
        ['400_error', [400,MailchimpBadRequestError], 'HTTP-error-code: 400, Error: Mailchimp Client faced a bad request.'],
        ['401_error', [401,MailchimpUnAuthorizedError], 'HTTP-error-code: 401, Error: The API key is either invalid or disabled.'],
        ['403_error', [403,MailchimpForbiddenError], 'HTTP-error-code: 403, Error: User does not have access to the requested operation.'],
        ['404_error', [404,MailchimpNotFoundError], 'HTTP-error-code: 404, Error: The requested resource could not be found.'],
        ['405_error', [405,MailchimpMethodNotAllowedError], 'HTTP-error-code: 405, Error: The resource does not accept the HTTP method.'],
        ['414_error', [414,MailchimpNestingError], 'HTTP-error-code: 414, Error: The sub-resource requested is nested too deeply.'],
        ['422_error', [422,MailchimpInvalidMethodError], 'HTTP-error-code: 422, Error: You can only use the X-HTTP-Method-Override header with the POST method.'],
        ['426_error', [426,MailchimpUpgradeError], 'HTTP-error-code: 426, Error: Your request was made with the HTTP protocol. Please make your request via HTTPS rather than HTTP.'],
        ['429_error', [429,MailchimpRateLimitError], 'HTTP-error-code: 429, Error: You have exceeded the limit of 10 simultaneous connections.'],
        ['426_error', [500,MailchimpInternalServerError], 'HTTP-error-code: 500, Error: A deep internal error has occurred during the processing of your request.'],
        ['5xx_error', [509,Server5xxError], 'HTTP-error-code: 509, Error: Unknown Error'],
        ['456_error', [456,MailchimpError], 'HTTP-error-code: 456, Error: Unknown Error'],
    ])
    
    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request")
    def test_custom_response_message(self,name, test_data, expected_message, mocked_request, mocked_sleep):
        resp_str = {}
        mocked_request.return_value = get_mock_http_response(test_data[0],json.dumps(resp_str))

        with self.assertRaises(test_data[1]) as e:
            self.mailchimp_client.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), expected_message)

class TestResponseErrorHandling(unittest.TestCase):
    """Test cases to verify the error from the API are displayed as expected"""

    config = {
        "access_token": "test_access_token",
    }

    mailchimp_client = client.MailchimpClient(config)
    method = 'GET'
    path = 'path'
    url = 'url'

    @parameterized.expand([
        ['400_error',[400,MailchimpBadRequestError,{"detail":'Bad Request Error'}],"HTTP-error-code: 400, Error: Bad Request Error"],
        ['401_error',[401,MailchimpUnAuthorizedError,{"detail":'Unauthorized Error'}],"HTTP-error-code: 401, Error: Unauthorized Error"],
        ['403_error',[403,MailchimpForbiddenError,{"detail":'Forbidden Error'}],"HTTP-error-code: 403, Error: Forbidden Error"],
        ['404_error',[404,MailchimpNotFoundError,{"detail":'Not found error'}],"HTTP-error-code: 404, Error: Not found error"],
        ['405_error',[405,MailchimpMethodNotAllowedError,{"detail":'Method not allowed Error'}],"HTTP-error-code: 405, Error: Method not allowed Error"],
        ['414_error',[414,MailchimpNestingError,{"detail":'Nesting Error'}],"HTTP-error-code: 414, Error: Nesting Error"],
        ['422_error',[422,MailchimpInvalidMethodError,{"detail":'Invalid method error'}],"HTTP-error-code: 422, Error: Invalid method error"],
        ['426_error',[426,MailchimpUpgradeError,{"detail":'Protocol upgrade error'}],"HTTP-error-code: 426, Error: Protocol upgrade error"],
        ['429_error',[429,MailchimpRateLimitError,{"detail":'Rate Limit Error'}],"HTTP-error-code: 429, Error: Rate Limit Error"],
        ['500_error',[500,MailchimpInternalServerError,{"detail":'Internal Server Error'}],"HTTP-error-code: 500, Error: Internal Server Error"],
    ])
    
    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request")
    def test_response_message(self,name,test_data, expected_message, mocked_request, mocked_sleep):
        """
        Exception with response message should be raised if status code returned from API
        """
        resp_str = test_data[2]
        mocked_request.return_value = get_mock_http_response(test_data[0],json.dumps(resp_str))

        with self.assertRaises(test_data[1]) as e:
            self.mailchimp_client.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), expected_message)

class TestJsonDecodeError(unittest.TestCase):
    """Test Case to Verify JSON Decode Error"""

    config = {
        "access_token": "test_access_token",
    }

    mailchimp_client = client.MailchimpClient(config)
    method = 'GET'
    path = 'path'
    url = 'url'
    
    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request")
    def test_json_decode_failed_4XX(self, mocked_request, mocked_sleep):
        """
        Exception with Unknown error message should be raised if invalid JSON response returned with 4XX error
        """
        json_decode_error_str = "json_error"
        mocked_request.return_value = get_mock_http_response(
            400, json_decode_error_str)

        expected_message = "HTTP-error-code: 400, Error: Mailchimp Client faced a bad request."

        with self.assertRaises(client.MailchimpBadRequestError) as e:
            self.mailchimp_client.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), expected_message)

    @mock.patch("requests.Session.request")
    def test_authentication_in_discover_mode(self,mocked_request):
        """
        Appropriate error message should be raised in discover_mode if invalid credentials are passed.
        """
        mocked_request.return_value = get_mock_http_response(
            400, '{"detail":}')

        expected_message = "Error testing Mailchimp authentication. Error: MailchimpBadRequestError: HTTP-error-code: 400, Error: Mailchimp Client faced a bad request."

        with self.assertRaises(Exception) as e:
            do_discover(self.mailchimp_client)

        self.assertEqual(str(e.exception), expected_message)
