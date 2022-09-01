import json
import time
import random
import tarfile

import singer
from singer import metrics, metadata, Transformer
from singer.utils import strptime_to_utc, should_sync_field
from requests.exceptions import HTTPError

LOGGER = singer.get_logger()

MIN_RETRY_INTERVAL = 2 # 2 seconds
MAX_RETRY_INTERVAL = 300 # 5 minutes
MAX_RETRY_ELAPSED_TIME = 43200 # 12 hours

# Break up reports_email_activity batches to iterate over chunks
EMAIL_ACTIVITY_BATCH_SIZE = 100

class BatchExpiredError(Exception):
    pass

def next_sleep_interval(previous_sleep_interval):
    """Function to send the time to sleep based on previous sleep interval"""
    min_interval = previous_sleep_interval or MIN_RETRY_INTERVAL
    max_interval = previous_sleep_interval * 2 or MIN_RETRY_INTERVAL
    return min(MAX_RETRY_INTERVAL, random.randint(min_interval, max_interval))

def chunk_campaigns(sorted_campaigns, chunk_bookmark):
    """Function to break list for sorted campaigns into the batch size and send the chunked list"""
    chunk_start = chunk_bookmark * EMAIL_ACTIVITY_BATCH_SIZE
    chunk_end = chunk_start + EMAIL_ACTIVITY_BATCH_SIZE

    if chunk_bookmark > 0:
        LOGGER.info("reports_email_activity - Resuming requests starting at campaign_id %s (index %s) in chunks of %s",
                    sorted_campaigns[chunk_start],
                    chunk_start,
                    EMAIL_ACTIVITY_BATCH_SIZE)

    done = False
    while not done:
        current_chunk = sorted_campaigns[chunk_start:chunk_end]
        done = len(current_chunk) == 0
        if not done:
            end_index = min(chunk_end, len(sorted_campaigns))
            LOGGER.info("reports_email_activity - Will request for campaign_ids from %s to %s (index %s to %s)",
                        sorted_campaigns[chunk_start],
                        sorted_campaigns[end_index - 1],
                        chunk_start,
                        end_index - 1)
            yield current_chunk
        chunk_start = chunk_end
        chunk_end += EMAIL_ACTIVITY_BATCH_SIZE

def transform_activities(records):
    """Function to move activity at the top-level from the email activity records"""
    for record in records:
        if 'activity' in record:
            if '_links' in record:
                del record['_links']
            record_template = dict(record)
            del record_template['activity']

            for activity in record['activity']:
                new_activity = dict(record_template)
                for key, value in activity.items():
                    new_activity[key] = value
                yield new_activity

def nested_set(dic, path, value):
    """Function to set bookmark of child stream for every parent ids"""
    for key in path[:-1]:
        dic = dic.setdefault(key, {})
    dic[path[-1]] = value

# pylint: disable=too-many-instance-attributes
class BaseStream:
    stream_name = None
    key_properties = None
    replication_keys = []
    replication_method = None
    path = None
    params = {}
    data_key = None
    extra_fields = None
    child = []
    streams_to_sync = []
    bookmark_path = None
    bookmark_query_field = None
    report_streams = []

    def __init__(self, state, client, config, catalog, selected_stream_names, child_streams_to_sync):
        self.state = state
        self.client = client
        self.config = config
        self.catalog = catalog
        self.stream = self.catalog.get_stream(self.stream_name)
        self.selected_stream_names = selected_stream_names
        self.to_write_records = self.stream_name in selected_stream_names
        self.child_streams_to_sync = child_streams_to_sync

    def get_path(self, parent_id, child_stream_obj):
        """Function to return the API URL path based on the parent ids"""
        if child_stream_obj.stream_name in ['unsubscribes', 'reports_email_activity']:
            return child_stream_obj.path.format(parent_id)
        return self.path + '/' + str(parent_id) + child_stream_obj.path

    @classmethod
    def write_schema(cls, catalog):
        stream = catalog.get_stream(cls.stream_name)
        schema = stream.schema.to_dict()
        singer.write_schema(cls.stream_name, schema, stream.key_properties)

    def process_records(self, records, max_bookmark_field=None):
        """Function to transform and write records and get the maximum bookmark value"""
        stream = self.stream
        schema = stream.schema.to_dict()
        stream_metadata = metadata.to_map(stream.metadata)
        with metrics.record_counter(self.stream_name) as counter, Transformer() as transformer:
            for record in records:
                if self.replication_keys and (max_bookmark_field is None or record[self.replication_keys[0]] > max_bookmark_field):
                    max_bookmark_field = record[self.replication_keys[0]]
                record = transformer.transform(record,
                                               schema,
                                               stream_metadata)
                singer.write_record(self.stream_name, record)
                counter.increment()
            return max_bookmark_field

    def get_bookmark(self, path, default):
        """Function to get the bookmark at a particular path from the state"""
        dic = self.state
        for key in ['bookmarks'] + path:
            if key in dic:
                dic = dic[key]
            else:
                return default
        return dic

    def write_bookmark(self, path, value):
        nested_set(self.state, ['bookmarks'] + path, value)
        singer.write_state(self.state)

    def format_selected_fields(self):
        """Given a catalog with selected metadata return a comma separated string
        of the `data_key.field_name` for every selected field in the catalog plus
        the `extra_fields` defined below

        The result of this function is expected to be passed to the API as the
        value of the `fields` parameter

        We add '<data_key>' to the beginning of all the selected fields because
        fields are nested under the '<data_key>' key in the response object.
        """
        mdata = metadata.to_map(self.stream.metadata)
        fields = self.stream.schema.properties.keys()
        formatted_field_names = []
        for field in fields:
            field_metadata = mdata.get(('properties', field))
            if should_sync_field(field_metadata.get('inclusion'), field_metadata.get('selected')):
                formatted_field_names.append(self.data_key+'.'+field)

        default_fields = ['_links', 'total_items', 'constraints', self.data_key+'.'+'_links']
        formatted_field_names += default_fields
        if self.extra_fields:
            formatted_field_names += self.extra_fields
        return ",".join(formatted_field_names)

    def sync_substream(self, child, parent_record_id):
        """Function to update the API URL based on parent id and sync child stream"""
        child_stream_obj = STREAMS.get(child)(self.state, self.client, self.config, self.catalog, self.selected_stream_names, self.child_streams_to_sync)
        if child_stream_obj.replication_method == 'INCREMENTAL':
            child_stream_obj.bookmark_path[1] = parent_record_id
        child_stream_obj.path = self.get_path(parent_record_id, child_stream_obj)
        child_stream_obj.sync()

class FullTable(BaseStream):
    replication_method = 'FULL_TABLE'

    def sync(self):
        """Function to sync records and call child stream for every records"""

        ids = []
        page_size = int(self.config.get('page_size', '1000'))
        offset = 0
        has_more = True
        while has_more:
            params = {
                'count': page_size,
                'offset': offset,
                **self.params
            }

            LOGGER.info('%s - Syncing - count: %s, offset: %s', self.stream_name, page_size, offset)

            formatted_selected_fields = self.format_selected_fields()

            params['fields'] = formatted_selected_fields
            data = self.client.get(
                self.path,
                params=params,
                endpoint=self.stream_name)

            raw_records = data.get(self.data_key)

            if len(raw_records) < page_size:
                has_more = False

            for record in raw_records:
                del record['_links']
                ids.append(record.get('id'))
                if self.child:
                    for child in self.child:
                        if child in self.report_streams:
                            continue
                        if child in self.selected_stream_names or child in self.child_streams_to_sync:
                            self.sync_substream(child, record.get('id'))
            if self.to_write_records:
                self.process_records(raw_records)

            for report_stream in self.report_streams:
                if report_stream not in self.selected_stream_names:
                    continue
                reports_stream_obj = STREAMS.get(report_stream)(self.state, self.client, self.config, self.catalog, self.selected_stream_names, self.child_streams_to_sync)
                reports_stream_obj.sync_report_activities(ids)

            offset += page_size

class Incremental(BaseStream):
    replication_method = 'INCREMENTAL'

    def sync(self):
        last_datetime = self.get_bookmark(self.bookmark_path, self.config.get('start_date'))
        max_bookmark_field = last_datetime
        ids = []

        page_size = int(self.config.get('page_size', '1000'))
        offset = 0
        has_more = True
        while has_more:
            params = {
                'count': page_size,
                'offset': offset,
                **self.params
            }

            params[self.bookmark_query_field] = last_datetime

            LOGGER.info('%s - Syncing - %scount: %s, offset: %s', self.stream_name, 'since: {}, '.format(last_datetime), page_size, offset)

            formatted_selected_fields = self.format_selected_fields()

            params['fields'] = formatted_selected_fields
            data = self.client.get(
                self.path,
                params=params,
                endpoint=self.stream_name)

            raw_records = data.get(self.data_key)

            if len(raw_records) < page_size:
                has_more = False

            for record in raw_records:
                del record['_links']
                ids.append(record.get('id'))
                if self.child:
                    for child in self.child:
                        if child in self.report_streams:
                            continue
                        if child in self.selected_stream_names or child in self.child_streams_to_sync:
                            self.sync_substream(child, record.get('id'))
            if self.to_write_records:
                max_bookmark_field = self.process_records(raw_records, max_bookmark_field)

            for report_stream in self.report_streams:
                if report_stream not in self.selected_stream_names:
                    continue
                reports_stream_obj = STREAMS.get(report_stream)(self.state, self.client, self.config, self.catalog, self.selected_stream_names, self.child_streams_to_sync)
                reports_stream_obj.sync_report_activities(ids)

            self.write_bookmark(self.bookmark_path, max_bookmark_field)

            offset += page_size

class Automations(FullTable):
    stream_name = 'automations'
    data_key = stream_name
    key_properties = ['id']
    path = '/automations'

class Lists(FullTable):
    stream_name = 'lists'
    key_properties = ['id']
    path = '/lists'
    params = {
        'sort_field': 'date_created',
        'sort_dir': 'ASC'
    }
    child = ['list_segments', 'list_members']
    data_key = stream_name

class ListMembers(Incremental):
    stream_name = 'list_members'
    key_properties = ['id', 'list_id']
    path = '/members'
    streams_to_sync = ['lists']
    data_key = 'members'
    bookmark_path = ['lists', '', stream_name, 'datetime']
    bookmark_query_field = 'since_last_changed'
    replication_keys = ['last_changed']

class ListSegments(FullTable):
    stream_name = 'list_segments'
    key_properties = ['id']
    path = '/segments'
    streams_to_sync = ['lists']
    data_key = 'segments'
    child = ['list_segment_members']

class ListSegmentMembers(FullTable):
    stream_name = 'list_segment_members'
    key_properties = ['id']
    path = '/members'
    streams_to_sync = ['lists', 'list_segments']
    data_key = 'members'

class Campaigns(FullTable):
    stream_name = 'campaigns'
    key_properties = ['id']
    path = '/campaigns'
    params = {
        'status': 'sent',
        'sort_field': 'send_time',
        'sort_dir': 'ASC'
    }
    child = ['reports_email_activity', 'unsubscribes']
    report_streams = ['reports_email_activity']
    data_key = stream_name

class Unsubscribes(FullTable):
    stream_name = 'unsubscribes'
    key_properties = ['campaign_id', 'email_id']
    path = '/reports/{}/unsubscribed'
    streams_to_sync = ['campaigns']
    data_key = stream_name

class ReportEmailActivity(Incremental):
    stream_name = 'reports_email_activity'
    extra_fields = ['emails.activity']
    key_properties = ['campaign_id', 'action', 'email_id', 'timestamp']
    streams_to_sync = ['campaigns']
    path = '/reports/{}/email-activity'
    data_key = 'emails'
    replication_keys = ['timestamp']

    def write_activity_batch_bookmark(self, batch_id):
        """Write batch id bookmark"""
        self.write_bookmark(['reports_email_activity_last_run_id'], batch_id)

    def write_email_activity_chunk_bookmark(self, current_bookmark, current_index, sorted_campaigns):
        """Write chunk bookmark for email activities"""
        # Bookmark the next chunk because the current chunk will be saved in batch_id
        # Index is relative to current bookmark
        next_chunk = current_bookmark + current_index + 1
        if next_chunk * EMAIL_ACTIVITY_BATCH_SIZE < len(sorted_campaigns):
            self.write_bookmark(['reports_email_activity_next_chunk'], next_chunk)
        else:
            self.write_bookmark(['reports_email_activity_next_chunk'], 0)

    def get_batch_info(self, batch_id):
        """Function to get batch status"""
        try:
            return self.client.get('/batches/{}'.format(batch_id), endpoint='get_batch_info')
        except HTTPError as e:
            if e.response.status_code == 404:
                raise BatchExpiredError('Batch {} expired'.format(batch_id))
            raise e

    def check_and_resume_email_activity_batch(self):
        """Function to resume batch syncing from previous sync"""
        batch_id = self.get_bookmark(['reports_email_activity_last_run_id'], None)

        if batch_id:
            try:
                data = self.get_batch_info(batch_id)
                if data['status'] == 'finished' and not data['response_body_url']:
                    LOGGER.info('reports_email_activity - Previous run from state (%s) is empty, retrying.',
                                batch_id)
                    return
            except BatchExpiredError:
                LOGGER.info('reports_email_activity - Previous run from state expired: %s',
                            batch_id)
                return

            # Resume from bookmarked job_id, then if completed, issue a new batch for processing.
            campaigns = [] # Don't need a list of campaigns if resuming
            self.sync_email_activities(campaigns, batch_id)

    def poll_email_activity(self, batch_id):
        """Get the email activity URL after the batch as executed"""
        sleep = 0
        start_time = time.time()
        while True:
            data = self.get_batch_info(batch_id)

            ## needs to update frequently for target-stitch to capture state
            self.write_activity_batch_bookmark(batch_id)

            progress = ''
            if data['total_operations'] > 0:
                progress = ' ({}/{} {:.2f}%)'.format(
                    data['finished_operations'],
                    data['total_operations'],
                    (data['finished_operations'] / data['total_operations']) * 100.0)

            LOGGER.info('reports_email_activity - Job polling: %s - %s%s',
                        data['id'],
                        data['status'],
                        progress)

            if data['status'] == 'finished':
                return data
            elif (time.time() - start_time) > MAX_RETRY_ELAPSED_TIME:
                message = 'Mailchimp campaigns export is still in progress after {} seconds. Will continue with this export on the next sync.'.format(MAX_RETRY_ELAPSED_TIME)
                LOGGER.error(message)
                raise Exception(message)

            sleep = next_sleep_interval(sleep)
            LOGGER.info('campaigns - status: %s, sleeping for %s seconds',
                        data['status'],
                        sleep)
            time.sleep(sleep)

    def stream_email_activity(self, archive_url):
        """Get the Email Activities from the provided URL after batch completion"""

        failed_campaign_ids = []

        with self.client.request('GET', url=archive_url, s3=True, endpoint='s3') as response:
            with tarfile.open(mode='r|gz', fileobj=response.raw) as tar:
                file = tar.next()
                while file:
                    if file.isfile():
                        raw_operations = tar.extractfile(file)
                        operations = json.loads(raw_operations.read().decode('utf-8'))
                        for i, operation in enumerate(operations):
                            campaign_id = operation['operation_id']
                            last_bookmark = self.state.get('bookmarks', {}).get(self.stream_name, {}).get(campaign_id)
                            LOGGER.info("reports_email_activity - [batch operation %s] Processing records for campaign %s", i, campaign_id)
                            if operation['status_code'] != 200:
                                failed_campaign_ids.append(campaign_id)
                            else:
                                response = json.loads(operation['response'])
                                email_activities = response['emails']

                                max_bookmark_field = self.process_records(transform_activities(email_activities), last_bookmark)
                                self.write_bookmark([self.stream_name, campaign_id], max_bookmark_field)
                    file = tar.next()
        return failed_campaign_ids

    def sync_email_activities(self, campaign_ids, batch_id=None):
        """Sync email activities ie. create the batch, get response URL and fetch data from the response URL"""
        if batch_id:
            LOGGER.info('reports_email_activity - Picking up previous run: %s', batch_id)
        else:
            LOGGER.info('reports_email_activity - Starting sync')

            formatted_field_names = self.format_selected_fields()

            operations = []
            for campaign_id in campaign_ids:
                since = self.get_bookmark(['reports_email_activity', campaign_id], self.config.get('start_date'))
                operations.append({
                    'method': 'GET',
                    'path': self.path.format(campaign_id),
                    'operation_id': campaign_id,
                    'params': {
                        'since': since,
                        'fields': formatted_field_names
                    }
                })

            data = self.client.post('/batches', json={'operations': operations}, endpoint='create_actvity_export')

            batch_id = data['id']

            LOGGER.info('reports_email_activity - Job running: %s', batch_id)

            self.write_activity_batch_bookmark(batch_id)

        data = self.poll_email_activity(batch_id)

        LOGGER.info('reports_email_activity - Batch job complete: took %.2fs minutes',
                    (strptime_to_utc(data['completed_at']) - strptime_to_utc(data['submitted_at'])).total_seconds() / 60)

        failed_campaign_ids = self.stream_email_activity(data['response_body_url'])
        if failed_campaign_ids:
            LOGGER.warning("reports_email_activity - operations failed for campaign_ids: %s", failed_campaign_ids)

        self.write_activity_batch_bookmark(None)

    def sync_report_activities(self, campaign_ids):
        """Function to loop over chunk of campaigns and sync email activities"""
        # Resume the previous batch, if necessary
        self.check_and_resume_email_activity_batch()

        # Chunk batch_ids, bookmarking the chunk number
        sorted_campaigns = sorted(campaign_ids)
        chunk_bookmark = int(self.get_bookmark(['reports_email_activity_next_chunk'], 0))
        for i, campaign_chunk in enumerate(chunk_campaigns(sorted_campaigns, chunk_bookmark)):
            self.write_email_activity_chunk_bookmark(chunk_bookmark, i, sorted_campaigns)
            self.sync_email_activities(campaign_chunk)

        # Start from the beginning next time
        self.write_bookmark(['reports_email_activity_next_chunk'], 0)

STREAMS = {
    'automations': Automations,
    'lists': Lists,
    'list_segments': ListSegments,
    'list_segment_members': ListSegmentMembers,
    'list_members': ListMembers,
    'campaigns': Campaigns,
    'unsubscribes': Unsubscribes,
    'reports_email_activity': ReportEmailActivity
}
