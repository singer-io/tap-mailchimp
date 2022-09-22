import unittest
from unittest import mock
from parameterized import parameterized
from tap_mailchimp.client import MailchimpClient
from tap_mailchimp.streams import ReportEmailActivity


class Catalog:
    '''
        Class to provide required attributes for test cases.
    '''
    def get_stream(*args):
        return None


class TestChunkSize(unittest.TestCase):
    '''
        Test class to verify that the chunk_size is selected appropriately.
    '''

    config = {
        "access_token": "test_access_token",
        "client_secret": "test_client_secret",
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

        client = MailchimpClient(config={**self.config, **test_data[0]})
        reports_email_activities = ReportEmailActivity(
            state={},
            client=client,
            config={**self.config, **test_data[0]},
            catalog=Catalog(),
            selected_stream_names=[],
            child_streams_to_sync=[]
        )
        reports_email_activities.write_email_activity_chunk_bookmark(
            current_bookmark=0,
            current_index=test_data[1],
            sorted_campaigns=sorted_campaigns
        )

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

        client = MailchimpClient(config={**self.config, **test_data})
        reports_email_activities = ReportEmailActivity(
            state={},
            client=client,
            config={**self.config, **test_data},
            catalog=Catalog(),
            selected_stream_names=[],
            child_streams_to_sync=[]
        )
        chunk_campaigns = reports_email_activities.chunk_campaigns(
            sorted_campaigns=sorted_campaigns,
            chunk_bookmark=0
        )

        # Verify the chunk_size value
        self.assertEqual(reports_email_activities.chunk_size, expected_data[0])
        # Verify the list of campaign chunks
        self.assertEqual(len(list(chunk_campaigns)), expected_data[1])

    @parameterized.expand([
        ['zero_int_chunk_size', {'chunk_size': 0},
            'The chunk_size cannot be Zero(0).'],
        ['zero_string_chunk_size', {'chunk_size': '0'},
            'The chunk_size cannot be Zero(0).'],
        ['negative_int_chunk_size', {'chunk_size': -5},
            'The chunk_size cannot be negative.'],
        ['negative_int_string_chunk_size', {'chunk_size': '-5'},
            'The chunk_size cannot be negative.']
    ])
    @mock.patch("tap_mailchimp.streams.LOGGER.info")
    def test_chunk_size_error_with_logger(self, name, test_data, expected_data, mocked_logger_info):
        '''
            Test cases to verify that an exception is raised and corresponding logger is called
            when zero value or negative integer value is passed to the chunk_size parameter
            in config.
        '''

        client = MailchimpClient(config={**self.config, **test_data})

        # Verify we raise ValueError
        with self.assertRaises(ValueError) as e:
            ReportEmailActivity(
                state={},
                client=client,
                config={**self.config, **test_data},
                catalog=Catalog(),
                selected_stream_names=[],
                child_streams_to_sync=[]
            )

        # Verify the error message
        self.assertEqual(str(e.exception), 'Please provide a valid integer value for the chunk_size parameter.')

        # Verify we get the logger when Negative and Zero(0) chunk_size is passed in the config
        mocked_logger_info.assert_called_with(expected_data)

    @parameterized.expand([
        ['positive_float_chunk_size', {'chunk_size': 5.1}],
        ['positive_float_string_chunk_size', {'chunk_size': '5.1'}],
        ['string_chunk_size', {'chunk_size': 'test'}],
        ['negative_float_chunk_size', {'chunk_size': -5.1}],
        ['negative_float_string_chunk_size', {'chunk_size': '-5.1'}],
        ['positive_float_near_zero_chunk_size', {'chunk_size': 0.1}],
        ['positive_float_string_near_zero_chunk_size', {'chunk_size': '0.1'}],
        ['negative_float_near_zero_chunk_size', {'chunk_size': -0.1}],
        ['negative_float_string_near_zero_chunk_size', {'chunk_size': '-0.1'}],
    ])
    def test_chunk_size_error_without_logger(self, name, test_data):
        '''
            Test cases to verify that an exception is raised when floating point
            value or string value is passed to the chunk_size parameter in config.
        '''

        client = MailchimpClient(config={**self.config, **test_data})

        # Verify we raise ValueError
        with self.assertRaises(ValueError) as e:
            ReportEmailActivity(
                state={},
                client=client,
                config={**self.config, **test_data},
                catalog=Catalog(),
                selected_stream_names=[],
                child_streams_to_sync=[]
            )

        # Verify the error message
        self.assertEqual(str(e.exception), 'Please provide a valid integer value for the chunk_size parameter.')
