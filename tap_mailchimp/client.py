import functools
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

class MailchimpError(Exception):
    pass

class MailchimpBadRequestError(MailchimpError):
    pass

class MailchimpUnAuthorizedError(MailchimpError):
    pass

class MailchimpForbiddenError(MailchimpError):
    pass

class MailchimpNotFoundError(MailchimpError):
    pass

class MailchimpMethodNotAllowedError(MailchimpError):
    pass

class MailchimpNestingError(MailchimpError):
    pass

class MailchimpInvalidMethodError(MailchimpError):
    pass

class MailchimpUpgradeError(MailchimpError):
    pass

class MailchimpRateLimitError(ClientRateLimitError):
    pass

class MailchimpInternalServerError(Server5xxError):
    pass

# Error glossary: https://mailchimp.com/developer/marketing/docs/errors/
ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": MailchimpBadRequestError,
        "message": "Mailchimp Client faced a bad request."
    },
    401: {
        "raise_exception": MailchimpUnAuthorizedError,
        "message": "The API key is either invalid or disabled."
    },
    403: {
        "raise_exception": MailchimpForbiddenError,
        "message": "User does not have access to the requested operation."
    },
    404: {
        "raise_exception": MailchimpNotFoundError,
        "message": "The requested resource could not be found."
    },
    405: {
        "raise_exception": MailchimpMethodNotAllowedError,
        "message": "The resource does not accept the HTTP method."
    },
    414: {
        "raise_exception": MailchimpNestingError,
        "message": "The sub-resource requested is nested too deeply."
    },
    422: {
        "raise_exception": MailchimpInvalidMethodError,
        "message": "You can only use the X-HTTP-Method-Override header with the POST method."
    },
    426: {
        "raise_exception": MailchimpUpgradeError,
        "message": "Your request was made with the HTTP protocol. Please make your request via HTTPS rather than HTTP."
    },
    429: {
        "raise_exception": MailchimpRateLimitError,
        "message": "You have exceeded the limit of 10 simultaneous connections."
    },
    500: {
        "raise_exception": MailchimpInternalServerError,
        "message": "A deep internal error has occurred during the processing of your request."
    }}

def get_exception_for_error_code(status_code):
    """Function to retrieve exceptions based on status_code"""

    exception = ERROR_CODE_EXCEPTION_MAPPING.get(status_code, {}).get('raise_exception')
    if not exception:
        exception = Server5xxError if status_code > 500 else MailchimpError
    return exception

def raise_for_error(response):
    """Function to raise an error by extracting the message from the error response"""
    try:
        json_response = response.json()

    except Exception:
        json_response = {}

    status_code = response.status_code
    msg = json_response.get(
        "detail",
        ERROR_CODE_EXCEPTION_MAPPING.get(status_code, {}).get(
            "message", "Unknown Error"
        )
    )

    message = "HTTP-error-code: {}, Error: {}".format(status_code, msg)

    exc = get_exception_for_error_code(status_code)
    raise exc(message) from None

def retry_pattern(fnc):
    """Function for backoff"""
    @backoff.on_exception(backoff.expo,
                          (Server5xxError, MailchimpRateLimitError, ConnectionError, Timeout),
                          max_tries=6,
                          factor=3)
    @functools.wraps(fnc)
    def wrapper(*args, **kwargs):
        return fnc(*args, **kwargs)
    return wrapper

class MailchimpClient:
    def __init__(self, config):
        self.__user_agent = config.get('user_agent')
        self.__access_token = config.get('access_token')
        self.__api_key = config.get('api_key')
        self.__session = requests.Session()
        self.__base_url = None

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

        if response.status_code != 200:
            raise_for_error(response)

        if s3:
            return response

        return response.json()

    @retry_pattern
    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)

    @retry_pattern
    def post(self, path, **kwargs):
        return self.request('POST', path=path, **kwargs)
