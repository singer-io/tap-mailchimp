import unittest
from unittest import mock
from parameterized import parameterized
from tap_mailchimp.client import MailchimpClient
from tap_mailchimp.streams import ReportEmailActivity

class Catalog:
    def get_stream(*args, **kwargs):
        return None

class TestChunkSize(unittest.TestCase):
    config = {
        "access_token": "test_access_token",
        "client_secret":"test_client_secret",
        "start_date": "2013-01-01T00:00:00Z"
    }

    @parameterized.expand([
        ['chunk_size_not_present', [{}, 0], 0],
        ['empty_string_chunk_size', [{'chunk_size': ''}, 0], 0],
        ['positive_int_chunk_size_next_chunk_bookmark', [{'chunk_size': 5}, 0], 1],
        ['positive_int_chunk_size_empty_bookmark', [{'chunk_size': 5}, 1], 0],
        ['positive_int_string_chunk_size_next_chunk_bookmark', [{'chunk_size': '5'}, 0], 1],
        ['positive_int_string_chunk_size_empty_bookmark', [{'chunk_size': '5'}, 1], 0],
    ])
    @mock.patch("tap_mailchimp.streams.BaseStream.write_bookmark")
    def test_chunk_size_bookmark(self, name, test_data, expected_data, mocked_write_bookmark):
        '''
            Test case to verify we set valid bookmark as per the chunk_size
        '''

        sorted_campaigns = [1, 2, 3, 4, 5, 6, 7, 8]

        client = MailchimpClient({**self.config, **test_data[0]})
        reports_email_activities = ReportEmailActivity({}, client, {**self.config, **test_data[0]}, Catalog(), [], [])
        reports_email_activities.write_email_activity_chunk_bookmark(0, test_data[1], sorted_campaigns)

        args, kwargs = mocked_write_bookmark.call_args
        # Verify the chunk_size value
        self.assertEqual(args[1], expected_data)

    @parameterized.expand([
        ['chunk_size_not_present', {}, [100, 1]],
        ['positive_int_chunk_size', {'chunk_size': 5}, [5, 2]],
        ['positive_int_string_chunk_size', {'chunk_size': '5'}, [5, 2]],
        ['empty_string_chunk_size', {'chunk_size': ''}, [100, 1]],
    ])
    def test_chunk_size_valid(self, name, test_data, expected_data):
        '''
            Test case to verify we set the valid integer as the chunk_size
        '''

        sorted_campaigns = [1, 2, 3, 4, 5, 6, 7, 8]

        client = MailchimpClient({**self.config, **test_data})
        reports_email_activities = ReportEmailActivity({}, client, {**self.config, **test_data}, Catalog(), [], [])
        chunk_campaigns = reports_email_activities.chunk_campaigns(sorted_campaigns, 0)

        # Verify the chunk_size value
        self.assertEqual(reports_email_activities.chunk_size, expected_data[0])
        # Verify the list of campaign chunks
        self.assertEqual(len(list(chunk_campaigns)), expected_data[1])

    @parameterized.expand([
        ['zero_int_chunk_size', {'chunk_size': 0}, [True, 'The chunk_size cannot be Zero(0).']],
        ['zero_string_chunk_size', {'chunk_size': '0'}, [True, 'The chunk_size cannot be Zero(0).']],
        ['negative_int_chunk_size', {'chunk_size': -5}, [True, 'The chunk_size cannot be negative.']],
        ['negative_int_string_chunk_size', {'chunk_size': '-5'}, [True, 'The chunk_size cannot be negative.']],
        ['positive_float_chunk_size', {'chunk_size': 5.1}, [False]],
        ['positive_float_string_chunk_size', {'chunk_size': '5.1'}, [False]],
        ['string_chunk_size', {'chunk_size': 'test'}, [False]],
        ['negative_float_chunk_size', {'chunk_size': -5.1}, [False]],
        ['negative_float_string_chunk_size', {'chunk_size': '-5.1'}, [False]],
        ['positive_float_near_zero_chunk_size', {'chunk_size': 0.1}, [False]],
        ['positive_float_string_near_zero_chunk_size', {'chunk_size': '0.1'}, [False]],
        ['negative_float_near_zero_chunk_size', {'chunk_size': -0.1}, [False]],
        ['negative_float_string_near_zero_chunk_size', {'chunk_size': '-0.1'}, [False]],
    ])
    @mock.patch("tap_mailchimp.streams.LOGGER.info")
    def test_chunk_size_error(self, name, test_data, expected_data, mocked_logger_info):
        '''
            Test cases to verify we raise an error if invalid chunk_size is passed from the config
        '''

        client = MailchimpClient({**self.config, **test_data})

        # Verify we raise ValueError
        with self.assertRaises(ValueError) as e:
            ReportEmailActivity({}, client, {**self.config, **test_data}, Catalog(), [], [])

        # Verify the error message
        self.assertEqual(str(e.exception), 'Please provide a valid integer value for the chunk_size parameter.')

        # Verify we get the logger when Negative and Zero(0) chunk_size is passed in the config
        if expected_data[0]:
            mocked_logger_info.assert_called_with(expected_data[1])