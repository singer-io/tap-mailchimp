import backoff
import requests
import singer
from requests.exceptions import ConnectionError, Timeout # pylint: disable=redefined-builtin
from singer import metrics

LOGGER = singer.get_logger()

REQUEST_TIMEOUT = 300
class ClientRateLimitError(Exception):
    pass

class Server5xxError(Exception):
    pass

class MailchimpClient:
    def __init__(self, config):
        self.__user_agent = config.get('user_agent')
        self.__access_token = config.get('access_token')
        self.__api_key = config.get('api_key')
        self.__session = requests.Session()
        self.__base_url = None
        self.page_size = int(config.get('page_size', '1000'))

        # Set request timeout to config param `request_timeout` value.
        # If value is 0,"0","" or not passed then it set default to 300 seconds.
        config_request_timeout = config.get('request_timeout')
        if config_request_timeout and float(config_request_timeout):
            self.__request_timeout = float(config_request_timeout)
        else:
            self.__request_timeout = REQUEST_TIMEOUT

        if not self.__access_token and self.__api_key:
            self.__base_url = 'https://{}.api.mailchimp.com'.format(
                config.get('dc'))

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback): # pylint: disable=redefined-builtin
        self.__session.close()

    def get_base_url(self):
        data = self.request('GET',
                            url='https://login.mailchimp.com/oauth2/metadata',
                            endpoint='base_url')
        self.__base_url = data['api_endpoint']

    @backoff.on_exception(backoff.expo,
                          Timeout, # Backoff for request timeout
                          max_tries=5,
                          factor=2)
    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ClientRateLimitError, ConnectionError),
                          max_tries=6,
                          factor=3)
    def request(self, method, path=None, url=None, s3=False, **kwargs):
        if url is None and self.__base_url is None:
            self.get_base_url()

        if url is None and path:
            url = self.__base_url + '/3.0' + path

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        if not s3:
            if self.__access_token:
                kwargs['headers']['Authorization'] = 'OAuth {}'.format(self.__access_token)
            elif self.__api_key:
                kwargs['auth'] = ('', self.__api_key)
            else:
                raise Exception('`access_token` or `api_key` required')

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if s3:
            kwargs['stream'] = True

        with metrics.http_request_timer(endpoint) as timer:
            LOGGER.info("Executing %s request to %s with params: %s", method, url, kwargs.get('params'))
            response = self.__session.request(method, url, timeout=self.__request_timeout, **kwargs) # Pass request timeout
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code == 429:
            raise ClientRateLimitError()

        response.raise_for_status()

        if s3:
            return response

        return response.json()

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)

    def post(self, path, **kwargs):
        return self.request('POST', path=path, **kwargs)
