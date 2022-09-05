import unittest
from unittest import mock
import requests
from tap_mailchimp.client import MailchimpClient



class Mocked():
    '''
        Class to provide required attributes for test cases.
    '''

    status_code = 200



class TestClient(unittest.TestCase):
    '''
        Test class to verify proper working of functions in client.py file.
    '''
    config = {
            'user_agent' : 'test_agent',
            'access_token': 'test_token',
            'api_key': 'test_key',
            'request_timeout' : 0
        }

    obj = MailchimpClient(config)



    @mock.patch("tap_mailchimp.client.MailchimpClient.request")
    def test_get_base_url(self, mocked_request):
        '''
            Test case to verify that the base_url is set depending upon the endpoint.
        '''

        mocked_request.return_value = {'api_endpoint' : 'https://test.com'}

        self.obj.get_base_url()

        self.assertEqual(self.obj._MailchimpClient__base_url, 'https://test.com')



    def test_request(self):
        '''
            Test case to verify the working of request function.
        '''

        resp = self.obj.request('GET',None, 'https://mailchimp.com', {'endpoint':'base_url'})

        self.assertEqual(resp.status_code, 200)


    @mock.patch("tap_mailchimp.client.MailchimpClient.request")
    def test_get(self, mocked_request):
        '''
            Test case to verify the working of get function.
        '''

        mocked_request.return_value = Mocked
        resp = self.obj.get({'endpoint':'base_url'})

        self.assertEqual(resp.status_code, 200)



    @mock.patch("tap_mailchimp.client.MailchimpClient.request")
    def test_post(self, mocked_request):
        '''
            Test case to verify the working of post function.
        '''

        mocked_request.return_value = Mocked
        resp = self.obj.post({'endpoint':'base_url'})

        self.assertEqual(resp.status_code, 200)



    def test_mailchimp_client(self):
        '''
            Test case to verify that the client is initialized appropriately depending
            upon the provided config.
        '''

        test_config = {
            'user_agent' : 'test_agent',
            'request_timeout' : 0,
            'api_key': 'test_key',
            'dc': 'test'
        }

        _object = MailchimpClient(test_config)

        self.assertEqual(_object._MailchimpClient__base_url, 'https://test.api.mailchimp.com')


    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=requests.exceptions.Timeout)
    @mock.patch("tap_mailchimp.client.LOGGER")
    def test_request_no_url(self, mocked_logger, mocked_request, mocked_sleep):
        '''
            Test case to verify working of request function when no url is provided but
            path is provided.
        '''

        with self.assertRaises(requests.exceptions.Timeout):
            self.obj.request('GET', path = 'test/TEST')

        mocked_logger.info.assert_called_with(
            'Executing %s request to %s with params: %s', 'GET', 'https://test.com/3.0test/TEST',
            None)



    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=requests.exceptions.Timeout)
    @mock.patch("tap_mailchimp.client.LOGGER")
    def test_request_no_url_no_path(self, mocked_logger, mocked_request, mocked_sleep):
        '''
            Test case to verify the working of request function when neither url nor
            path is provided.
        '''

        config = {
            'user_agent' : 'test_agent',
            'access_token': 'test_token',
            'api_key': 'test_key',
            'request_timeout' : 0
        }

        obj = MailchimpClient(config)

        with self.assertRaises(requests.exceptions.Timeout):
            obj.request('GET', path = 'test/TEST')

        mocked_logger.info.assert_called_with(
            'Executing %s request to %s with params: %s', 'GET',
            'https://login.mailchimp.com/oauth2/metadata', None)




    def test_request_no_s3_raises_exception(self):
        '''
            Test case to verify that at least one of "access_token" or "api_key"
            must be provided. If not, then exception must be raised.
        '''

        config = {
            'user_agent' : 'test_agent',
            'request_timeout' : 0
        }
        obj = MailchimpClient(config)

        with self.assertRaises(Exception) as e:
            obj.request(method = 'GET',url = 'https://mailchimp.com')

        self.assertEqual(str(e.exception), '`access_token` or `api_key` required')
