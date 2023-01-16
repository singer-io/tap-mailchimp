import unittest
from unittest import mock
from requests.exceptions import HTTPError
from parameterized import parameterized
from tap_mailchimp import streams
from tap_mailchimp.client import MailchimpClient


class Mocked():
    '''
        Class to provide required attributes for test cases.
    '''
    config = {}
    params = {}
    stream_name = 'automations'


class Schema:
    '''
          Class to provide required attributes for test cases.
    '''

    def __init__(self, stream_name):
        self.stream_name = stream_name

    def to_dict(self):
        return {'stream': self.stream_name}


class Catalog:
    '''
        Class to provide required attributes for test cases.
    '''

    def __init__(self, stream_name):
        self.stream_name = stream_name

    def get_stream(self, stream_name):
        return Streams(self.stream_name)

    def to_dict(self):
        return {"field": "value"}


class Streams:
    '''
        Class to provide required attributes for test cases.
    '''

    def __init__(self, stream_name):
        self.stream_name = stream_name
        self.path = '/test_path'
        self.schema = Schema(stream_name)
        self.key_properties = []
        self.metadata = [
            {'breadcrumb': (), 'metadata': {'valid-replication-keys': []}}]


class MockResponse:
    '''
        Class for Response object
    '''

    def __init__(self, status_code):
        self.status_code = status_code


class StreamsTest(unittest.TestCase):
    '''
        Test class to verify working of functions in streams.py file.
    '''

    obj = streams.BaseStream(
        state='test_state',
        client=MailchimpClient,
        config={},
        catalog=Catalog('automations'),
        selected_stream_names=[Streams('automations')],
        child_streams_to_sync=None
    )

    @parameterized.expand([
        ['non_zero_value', 3],
        ['zero_value', 0]
    ])
    def test_next_sleep_interval(self, name, test_value_1):
        '''
            Test case to verify that the return value of next_sleep_interval is greater than or
            equal to the previous_sleep_interval. If previous_sleep_interval is not given (or zero
            is given) then it should take the default value specified.
        '''

        previous_sleep_interval = test_value_1
        next_sleep = streams.next_sleep_interval(
            previous_sleep_interval=previous_sleep_interval)
        self.assertGreaterEqual(next_sleep, previous_sleep_interval)

    @parameterized.expand([
        ['zero_chunk_bookmark', 0, 3],
        ['non_zero_chunk_bookmark', 2, 1]
    ])
    @mock.patch("tap_mailchimp.streams.LOGGER")
    def test_chunk_campaigns_chunk_bookmark(self, name, test_value_1, test_value_2, mocked_logger):
        '''
            Test case to verify that the chunk_campaigns function divides the provided
            sorted_campaigns starting from the provided chunk_bookmark.
        '''

        sorted_campaigns = []

        while len(sorted_campaigns) < 250:
            sorted_campaigns.append("016cb6c4e7")

        client = MailchimpClient(config={})
        stream = streams.ReportEmailActivity(
            state={},
            client=client,
            config={},
            catalog=Catalog('reports_email_activity'),
            selected_stream_names=[],
            child_streams_to_sync=[]
        )
        chunk_campaigns = stream.chunk_campaigns(
            sorted_campaigns=sorted_campaigns,
            chunk_bookmark=test_value_1
        )

        self.assertEqual(len(list(chunk_campaigns)), test_value_2)

    @parameterized.expand([
        [
            'non_empty_activity',
            [{
                'campaign_id': 'test1',
                'email_id': 'test2',
                'activity': [{"TEST_key": "TEST_val"}],
                '_links': [{'test_key': 'test_val'}]
            }],
            [{
                'campaign_id': 'test1',
                'email_id': 'test2',
                'TEST_key': 'TEST_val'
            }]
        ],
        [
            'empty_activity',
            [{
                'campaign_id': 'test2',
                'email_id': 'test2',
                'activity': [],
                '_links': [{'test_key': 'test_val'}]
            }],
            []
        ]
    ])
    def test_transform_activities(self, name, test_value1, test_value2):
        '''
            Test case to verify that 'activity' field in records is transformed correctly.
        '''

        transformed_record = streams.transform_activities(records=test_value1)

        self.assertEqual(list(transformed_record), test_value2)

    def test_nested_set(self):
        '''
           Test case to verify that in nested dictionaries, key provided at given
           path should be initialized with the given value.
        '''

        dic = {
            "test1": "TEST",
            "test2": {
                "test_key": "test_val"
            },
            "test3": "Test"
        }
        path = ["test2", "test_key"]
        value = None
        expected = {
            "test1": "TEST",
            "test2": {
                "test_key": None
            },
            "test3": "Test"
        }

        streams.nested_set(dic=dic, path=path, value=value)

        self.assertEqual(dic, expected)

    @parameterized.expand([
        ['test_stream_1', 'unsubscribes', '/test_path'],
        ['test_stream_2', 'list_segments', '/TEST/10133232/test_path']
    ])
    def test_get_path(self, name, stream, expected_path):
        '''
            Test case to verify that path is returned appropriately
            as per the given arguments.
        '''

        obj = streams.BaseStream(
            state='test_state',
            client='test_client',
            config='test_config',
            catalog=Catalog(stream),
            selected_stream_names=[Streams(stream)],
            child_streams_to_sync=None
        )

        streams.BaseStream.path = '/TEST'

        actual_path = obj.get_path(
            parent_id=10133232, child_stream_obj=Streams(stream))

        self.assertEqual(actual_path, expected_path)

    @mock.patch("singer.messages.write_message")
    def test_write_schema(self, mocked_message):
        '''
            Test case to verify that schema is written for given stream.
        '''

        obj = streams.BaseStream(
            state='test_state',
            client='test_client',
            config='test_config',
            catalog=Catalog('automations'),
            selected_stream_names=[Streams('automations')],
            child_streams_to_sync=None
        )

        obj.write_schema(catalog=Catalog('automations'))

        self.assertTrue(mocked_message.called)

    @parameterized.expand([
        ['initial_bookmark_less_than_record_bookmark', '2001-05-02', '2008-02-10'],
        ['initial_bookmark_greater_than_record_bookmark', '2022-07-01', '2022-07-01'],
        ['no_initial_bookmark', None, '2008-02-10']
    ])
    def test_process_records(self, name, initial_bookmark, expected_bookmark):
        '''
            Test case to verify that the records are processed and correct bookmark is
            returned depending upon the initial bookmark and the records.
        '''

        obj = streams.BaseStream(
            state='test_state',
            client='test_client',
            config='test_config',
            catalog=Catalog('automations'),
            selected_stream_names=[Streams('automations')],
            child_streams_to_sync=None
        )

        streams.BaseStream.replication_keys = ['last_changed']
        test_record = [
            {'last_changed': '2008-02-01'},
            {'last_changed': '2008-02-10'},
            {'last_changed': '2008-02-02'}
        ]

        actual_bookmark = obj.process_records(
            records=test_record, max_bookmark_field=initial_bookmark)

        self.assertEqual(actual_bookmark, expected_bookmark)

    @parameterized.expand([
        ['returning_default', ['lists'], '2001-01-01'],
        ['not_returning_default', [], {
            'datetime': '2015-03-06T16:03:01+00:00'}]
    ])
    def test_get_bookmark(self, name, path, expected_bookmark):
        '''
            Test case to verify that correct bookmark is returned depending upon
            the provided path.
        '''

        test_state = {'bookmarks': {'datetime': '2015-03-06T16:03:01+00:00'}}

        obj = streams.BaseStream(
            state=test_state,
            client='test_client',
            config='test_config',
            catalog=Catalog('automations'),
            selected_stream_names=[Streams('automations')],
            child_streams_to_sync=None
        )

        actual_bookmark = obj.get_bookmark(path=path, default='2001-01-01')

        self.assertEqual(actual_bookmark, expected_bookmark)

    @mock.patch("singer.messages.write_message")
    def test_write_bookmark(self, mocked_write_message):
        '''
            Test case to verify that the bookmark is written.
        '''

        self.obj.state = {
            "test1": "TEST",
            "test2": {
                "test_key": "test_val"
            },
            "test3": "Test"
        }
        self.obj.write_bookmark(path=["test2", "test_key"], value=None)

        self.assertTrue(mocked_write_message.called)

    @mock.patch("tap_mailchimp.streams.Incremental.sync")
    @mock.patch("tap_mailchimp.streams.BaseStream.get_path")
    def test_sync_substream(self, mocked_get_path, mocked_sync):
        '''
            Test case to verify that the child stream (sub_stream) is synced.
        '''

        self.obj.sync_substream(child='list_members',
                                parent_record_id=10133232)
        self.assertTrue(mocked_sync.called)

    @mock.patch("tap_mailchimp.streams.BaseStream.sync")
    @mock.patch("tap_mailchimp.streams.BaseStream.get_bookmark")
    def test_incremental_sync(self, mocked_get_bookmark, mocked_basestream_sync):
        '''
            Test case to verify the working of sync function for incremental stream.
        '''

        sync_object = streams.Incremental(
            state='test_state',
            client=MailchimpClient,
            config={},
            catalog=Catalog('list_members'),
            selected_stream_names=[Streams('list_members')],
            child_streams_to_sync=None
        )

        mocked_get_bookmark.return_value = '2001-01-01'

        sync_object.sync()

        self.assertTrue(mocked_basestream_sync.called)

    @mock.patch("tap_mailchimp.streams.BaseStream.write_bookmark")
    def test_write_activity_batch_bookmark(self, mocked_write_bookmark):
        '''
            Test case to verify that bookmark is written for 'reports_email_activity'
            stream.
        '''

        _object_ = streams.ReportEmailActivity

        _object_.write_activity_batch_bookmark(self.obj, batch_id='8vh837xfqd')

        self.assertTrue(mocked_write_bookmark.called)

    @parameterized.expand([
        ['test1', 500],
        ['test2', 25]
    ])
    @mock.patch("tap_mailchimp.streams.BaseStream.write_bookmark")
    def test_write_email_activity_chunk_bookmark(self, name, test_value1, mocked_write_bookmark):
        '''
            Test case to verify that bookmark is written for chunks of campaigns in
            'reports_email_activity' stream.
        '''

        client = MailchimpClient(config={})
        _object_ = streams.ReportEmailActivity(
            state={},
            client=client,
            config={},
            catalog=Catalog('reports_email_activity'),
            selected_stream_names=[],
            child_streams_to_sync=[]
        )
        sorted_campaigns_ = []

        while len(sorted_campaigns_) < test_value1:
            sorted_campaigns_.append("016cb6c4e7")

        _object_.write_email_activity_chunk_bookmark(
            current_bookmark=2,
            current_index=1,
            sorted_campaigns=sorted_campaigns_
        )

        self.assertTrue(mocked_write_bookmark.called)

    @mock.patch("tap_mailchimp.client.MailchimpClient.get")
    def test_get_batch_info(self, mocked_client_get):
        '''
            Test case to verify that error is raised when incorrect batch_id
            is provided.
        '''

        _object_ = streams.ReportEmailActivity
        mocked_client_get.side_effect = HTTPError(
            "HTTP Error: Not authorized.", response=MockResponse(401))

        with self.assertRaises(HTTPError) as e:
            _object_.get_batch_info(self.obj, batch_id='8vh837xfqd')

        self.assertEqual(str(e.exception), 'HTTP Error: Not authorized.')

    @mock.patch("tap_mailchimp.client.MailchimpClient.get")
    def test_get_batch_info_batch_expire_error(self, mocked_client_get):
        '''
            Test case to verify that the 'BatchExpiredError' error is raised when we encounter a 404 error.
        '''

        _object_ = streams.ReportEmailActivity

        mocked_client_get.side_effect = HTTPError(
            "HTTP Error: Not Found.", response=MockResponse(404))

        with self.assertRaises(streams.BatchExpiredError) as e:
            _object_.get_batch_info(self.obj, batch_id='8vh837xfqd')

        self.assertEqual(str(e.exception), 'Batch 8vh837xfqd expired')

    @mock.patch("tap_mailchimp.streams.ReportEmailActivity.sync_email_activities")
    @mock.patch("tap_mailchimp.streams.ReportEmailActivity.get_batch_info")
    @mock.patch("tap_mailchimp.streams.BaseStream.get_bookmark")
    @mock.patch("tap_mailchimp.streams.LOGGER.info")
    def test_check_and_resume_email_activity_batch(self, mocked_logger, mocked_get_bookmark,
                                                   mocked_get_batch_info, mocked_sync_email_activities):
        '''
            Test case to verify that a batch is checked and resumed depending upon the bookmark.
        '''

        _object_ = streams.ReportEmailActivity
        mocked_get_bookmark.return_value = '8vh837xfqd'
        mocked_get_batch_info.return_value = {
            'status': 'finished',
            'response_body_url': None
        }

        _object_.check_and_resume_email_activity_batch(_object_)

        mocked_logger.assert_called_with(
            'reports_email_activity - Previous run from state (%s) is empty, retrying.',
            '8vh837xfqd')

    @mock.patch("tap_mailchimp.streams.ReportEmailActivity.sync_email_activities")
    @mock.patch("tap_mailchimp.streams.ReportEmailActivity.get_batch_info")
    @mock.patch("tap_mailchimp.streams.BaseStream.get_bookmark")
    @mock.patch("tap_mailchimp.streams.LOGGER.info")
    def test_check_and_resume_email_activity_batch_batchexpired_error(self, mocked_logger, mocked_get_bookmark,
                                                                      mocked_get_batch_info, mocked_sync_email_activities):
        '''
            Test case to verify that a batch is checked and resumed depending upon the bookmark.
        '''

        _object_ = streams.ReportEmailActivity
        mocked_get_bookmark.return_value = '8vh837xfqd'
        mocked_get_batch_info.side_effect = streams.BatchExpiredError('Batch 8vh837xfqd expired')

        _object_.check_and_resume_email_activity_batch(_object_)

        mocked_logger.assert_called_with(
            'reports_email_activity - Previous run from state expired: %s',
            '8vh837xfqd')

    @mock.patch("tap_mailchimp.streams.ReportEmailActivity.write_activity_batch_bookmark")
    @mock.patch("tap_mailchimp.streams.ReportEmailActivity.get_batch_info")
    @mock.patch("tap_mailchimp.streams.LOGGER")
    def test_poll_email_activity(self, mocked_logger, mocked_get_batch_info, mocked_write_activity_batch_bookmark):
        '''
            Test case to verify that all required operations are executed for each batch
            and the progress is calculated accordingly.
        '''

        _object_ = streams.ReportEmailActivity(
            state='test_state',
            client=MailchimpClient,
            config={},
            catalog=Catalog('reports_email_activity'),
            selected_stream_names=[Streams('automations')],
            child_streams_to_sync=None
        )

        mocked_get_batch_info.side_effect = [
            {
                'id': 'test',
                'status': 'pending',
                'total_operations': 2,
                'finished_operations': 1
            },
            {
                'id': 'test',
                'status': 'finished',
                'total_operations': 2,
                'finished_operations': 2
            }
        ]

        expected_data = {
            'id': 'test',
            'status': 'finished',
            'total_operations': 2,
            'finished_operations': 2
        }

        actual_data = _object_.poll_email_activity(batch_id='8vh837xfqd')

        self.assertEqual(actual_data, expected_data)

    @parameterized.expand([
        ['no_batch_id', None, 2],
        ['batch_id', '8vh837xfqd', 1]
    ])
    @mock.patch("tap_mailchimp.streams.BaseStream.format_selected_fields")
    @mock.patch("tap_mailchimp.client.MailchimpClient.post")
    @mock.patch("tap_mailchimp.streams.ReportEmailActivity.write_activity_batch_bookmark")
    @mock.patch("tap_mailchimp.streams.ReportEmailActivity.poll_email_activity")
    @mock.patch("tap_mailchimp.streams.ReportEmailActivity.stream_email_activity")
    def test_sync_email_activities(self, name, test_value_1, test_value_2,
                                   mocked_stream_email_activity, mocked_poll_email_activity,
                                   mocked_write_activity_batch_bookmark,
                                   mocked_post, mocked_format_selected_fields):
        '''
            Test case to verify that all batches of campaigns for 'reports_email_activity' stream
            are synced and bookmark is written for the same.
        '''

        _object_ = streams.ReportEmailActivity(
            state='test_state',
            client=MailchimpClient,
            config={},
            catalog=Catalog('reports_email_activity'),
            selected_stream_names=[Streams('automations')],
            child_streams_to_sync=None
        )

        campaign_ids = ["016cb6c4e7", "016ab6c4e7", "096cb6c4e7"]
        mocked_post.return_value = {'id': '8vh837xfqd'}
        mocked_poll_email_activity.return_value = {
            'completed_at': '2012-12-12',
            'submitted_at': '2012-12-12',
            'response_body_url': 'test'
        }

        _object_.sync_email_activities(campaign_ids, batch_id=test_value_1)

        self.assertEqual(
            mocked_write_activity_batch_bookmark.call_count, test_value_2)

    @mock.patch("tap_mailchimp.streams.BaseStream.write_bookmark")
    @mock.patch("tap_mailchimp.streams.ReportEmailActivity.sync_email_activities")
    @mock.patch("tap_mailchimp.streams.ReportEmailActivity.write_email_activity_chunk_bookmark")
    def test_sync_report_activities(self, mocked_write_email_activity_chunk_bookmark,
                                    mocked_sync_email_activities, mocked_write_bookmark):
        '''
            Test case to verify that 'reports_email_activity' stream is synced.
        '''

        _object_ = streams.ReportEmailActivity(
            state='test_state',
            client=MailchimpClient,
            config={},
            catalog=Catalog('reports_email_activity'),
            selected_stream_names=[Streams('automations')],
            child_streams_to_sync=None
        )

        campaign_ids = ["016cb6c4e7", "016ab6c4e7", "096cb6c4e7"]

        _object_.sync_report_activities(campaign_ids=campaign_ids)

        self.assertTrue(mocked_write_bookmark.called)

    @mock.patch("tap_mailchimp.streams.LOGGER.info")
    @mock.patch("singer.write_record")
    @mock.patch("tap_mailchimp.streams.BaseStream.process_records")
    @mock.patch("tap_mailchimp.client.MailchimpClient.get")
    @mock.patch("tap_mailchimp.streams.BaseStream.format_selected_fields")
    @mock.patch("tap_mailchimp.streams.BaseStream.write_bookmark")
    @mock.patch("tap_mailchimp.streams.BaseStream.sync_substream")
    @mock.patch("tap_mailchimp.streams.ReportEmailActivity.sync_report_activities")
    def test_sync(self, mocked_sync_report_activities, mocked_sync_substream,
                  mocked_write_bookmark, mocked_format_selected_fields, mocked_client_get,
                  mocked_process_records, mocked_write_record, mocked_logger):
        '''
            Test case to verify working of sync function.
        '''

        mocked_client_get.return_value = {
            'test': [
                {
                    'campaigns': [],
                    'total_items': 0,
                    '_links': []
                }
            ]
        }
        mocked_format_selected_fields.return_value = '_links,campaigns._links,campaigns.\
            field1,campaigns.field2,constraints,total_items'

        _object = self.obj
        _object.selected_stream_names = [
            'campaigns', 'reports_email_activity', 'unsubscribes']
        _object.child = ['reports_email_activity', 'unsubscribes']
        _object.data_key = 'test'
        _object.bookmark_query_field = True
        _object.report_streams = ['reports_email_activity', 'test_stream']
        _object.to_write_records = True

        self.obj.sync(sync_start_date=None)

        self.assertTrue(mocked_write_bookmark.called)
        self.assertTrue(mocked_sync_substream.called)
        self.assertTrue(mocked_process_records.called)
