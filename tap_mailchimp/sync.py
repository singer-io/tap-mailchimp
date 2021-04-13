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
    min_interval = previous_sleep_interval or MIN_RETRY_INTERVAL
    max_interval = previous_sleep_interval * 2 or MIN_RETRY_INTERVAL
    return min(MAX_RETRY_INTERVAL, random.randint(min_interval, max_interval))

def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    singer.write_schema(stream_name, schema, stream.key_properties)

def process_records(catalog,
                    stream_name,
                    records,
                    persist=True,
                    bookmark_field=None,
                    max_bookmark_field=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)
    with metrics.record_counter(stream_name) as counter, Transformer() as transformer:
        for record in records:
            if bookmark_field:
                if max_bookmark_field is None or \
                   record[bookmark_field] > max_bookmark_field:
                    max_bookmark_field = record[bookmark_field]
            if persist:
                record = transformer.transform(record,
                                               schema,
                                               stream_metadata)
                singer.write_record(stream_name, record)
                counter.increment()
        return max_bookmark_field

def get_bookmark(state, path, default):
    dic = state
    for key in ['bookmarks'] + path:
        if key in dic:
            dic = dic[key]
        else:
            return default
    return dic

def nested_set(dic, path, value):
    for key in path[:-1]:
        dic = dic.setdefault(key, {})
    dic[path[-1]] = value

def write_bookmark(state, path, value):
    nested_set(state, ['bookmarks'] + path, value)
    singer.write_state(state)

def sync_endpoint(client,
                  catalog,
                  state,
                  start_date,
                  stream_name,
                  persist,
                  path,
                  data_key,
                  static_params,
                  bookmark_path,
                  bookmark_query_field,
                  bookmark_field):
    bookmark_path = bookmark_path + ['datetime']
    last_datetime = get_bookmark(state, bookmark_path, start_date)
    ids = []
    max_bookmark_field = last_datetime

    def transform(record):
        _id = record.get('id')
        if _id:
            ids.append(_id)
        del record['_links']
        return record

    write_schema(catalog, stream_name)

    page_size = client.page_size
    offset = 0
    has_more = True
    while has_more:
        params = {
            'count': page_size,
            'offset': offset,
            **static_params
        }

        if bookmark_query_field:
            params[bookmark_query_field] = last_datetime

        LOGGER.info('%s - Syncing - %scount: %s, offset: %s',
                    stream_name,
                    'since: {}, '.format(last_datetime) if bookmark_query_field else '',
                    page_size,
                    offset)

        formatted_selected_fields = format_selected_fields(catalog, stream_name, data_key)

        params['fields'] = formatted_selected_fields

        data = client.get(
            path,
            params=params,
            endpoint=stream_name)


        raw_records = data.get(data_key)

        if len(raw_records) < page_size:
            has_more = False

        max_bookmark_field = process_records(catalog,
                                             stream_name,
                                             map(transform, raw_records),
                                             persist=persist,
                                             bookmark_field=bookmark_field,
                                             max_bookmark_field=max_bookmark_field)

        if bookmark_field:
            write_bookmark(state,
                           bookmark_path,
                           max_bookmark_field)

        offset += page_size

    return ids

def get_dependants(endpoint_config):
    dependants = endpoint_config.get('dependants', [])
    for stream_name, child_endpoint_config in endpoint_config.get('children', {}).items():
        dependants.append(stream_name)
        dependants += get_dependants(child_endpoint_config)
    return dependants

def sync_stream(client,
                catalog,
                state,
                start_date,
                streams_to_sync,
                id_bag,
                stream_name,
                endpoint_config,
                bookmark_path=None,
                id_path=None):
    if not bookmark_path:
        bookmark_path = [stream_name]
    if not id_path:
        id_path = []

    dependants = get_dependants(endpoint_config)
    should_stream, should_persist = should_sync_stream(streams_to_sync,
                                                       dependants,
                                                       stream_name)
    if should_stream:
        path = endpoint_config.get('path').format(*id_path)
        stream_ids = sync_endpoint(client,
                                   catalog,
                                   state,
                                   start_date,
                                   stream_name,
                                   should_persist,
                                   path,
                                   endpoint_config.get('data_path', stream_name),
                                   endpoint_config.get('params', {}),
                                   bookmark_path,
                                   endpoint_config.get('bookmark_query_field'),
                                   endpoint_config.get('bookmark_field'))

        if endpoint_config.get('store_ids'):
            id_bag[stream_name] = stream_ids

        children = endpoint_config.get('children')
        if children:
            for child_stream_name, child_endpoint_config in children.items():
                for _id in stream_ids:
                    sync_stream(client,
                                catalog,
                                state,
                                start_date,
                                streams_to_sync,
                                id_bag,
                                child_stream_name,
                                child_endpoint_config,
                                bookmark_path=bookmark_path + [_id, child_stream_name],
                                id_path=id_path + [_id])

def get_batch_info(client, batch_id):
    try:
        return client.get(
            '/batches/{}'.format(batch_id),
            endpoint='get_batch_info')
    except HTTPError as e:
        if e.response.status_code == 404:
            raise BatchExpiredError('Batch {} expired'.format(batch_id))
        raise e

def write_activity_batch_bookmark(state, batch_id):
    write_bookmark(state, ['reports_email_activity_last_run_id'], batch_id)

def poll_email_activity(client, state, batch_id):
    sleep = 0
    start_time = time.time()
    while True:
        data = get_batch_info(client, batch_id)

        ## needs to update frequently for target-stitch to capture state
        write_activity_batch_bookmark(state, batch_id)

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

def stream_email_activity(client, catalog, state, archive_url):
    stream_name = 'reports_email_activity'

    def transform_activities(records):
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

    write_schema(catalog, stream_name)

    failed_campaign_ids = []
    with client.request('GET', url=archive_url, s3=True, endpoint='s3') as response:
        with tarfile.open(mode='r|gz', fileobj=response.raw) as tar:
            file = tar.next()
            while file:
                if file.isfile():
                    rawoperations = tar.extractfile(file)
                    operations = json.loads(rawoperations.read().decode('utf-8'))
                    for i, operation in enumerate(operations):
                        campaign_id = operation['operation_id']
                        last_bookmark = state.get('bookmarks', {}).get(stream_name, {}).get(campaign_id)
                        LOGGER.info("reports_email_activity - [batch operation %s] Processing records for campaign %s", i, campaign_id)
                        if operation['status_code'] != 200:
                            failed_campaign_ids.append(campaign_id)
                        else:
                            response = json.loads(operation['response'])
                            email_activities = response['emails']
                            max_bookmark_field = process_records(
                                catalog,
                                stream_name,
                                transform_activities(email_activities),
                                bookmark_field='timestamp',
                                max_bookmark_field=last_bookmark)
                            write_bookmark(state,
                                           [stream_name, campaign_id],
                                           max_bookmark_field)
                file = tar.next()
    return failed_campaign_ids

def sync_email_activity(client, catalog, state, start_date, campaign_ids, batch_id=None):
    if batch_id:
        LOGGER.info('reports_email_activity - Picking up previous run: %s', batch_id)
    else:
        LOGGER.info('reports_email_activity - Starting sync')

        extra_fields = ['emails.activity']

        formatted_field_names = format_selected_fields(catalog, 'reports_email_activity', 'emails', extra_fields)

        operations = []
        for campaign_id in campaign_ids:
            since = get_bookmark(state, ['reports_email_activity', campaign_id], start_date)
            operations.append({
                'method': 'GET',
                'path': '/reports/{}/email-activity'.format(campaign_id),
                'operation_id': campaign_id,
                'params': {
                    'since': since,
                    'fields': formatted_field_names
                }
            })

        data = client.post(
            '/batches',
            json={
                'operations': operations
            },
            endpoint='create_actvity_export')

        batch_id = data['id']

        LOGGER.info('reports_email_activity - Job running: %s', batch_id)

        write_activity_batch_bookmark(state, batch_id)

    data = poll_email_activity(client, state, batch_id)

    LOGGER.info('reports_email_activity - Batch job complete: took %.2fs minutes',
                (strptime_to_utc(data['completed_at']) - strptime_to_utc(data['submitted_at'])).total_seconds() / 60)

    failed_campaign_ids = stream_email_activity(client,
                                                catalog,
                                                state,
                                                data['response_body_url'])
    if failed_campaign_ids:
        LOGGER.warning("reports_email_activity - operations failed for campaign_ids: %s", failed_campaign_ids)

    write_activity_batch_bookmark(state, None)

def get_selected_streams(catalog):
    selected_streams = set()
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        root_metadata = mdata.get(())
        if root_metadata and root_metadata.get('selected') is True:
            selected_streams.add(stream.tap_stream_id)
    return list(selected_streams)

def format_selected_fields(catalog, stream_name, data_key, extra_fields=None):
    """Given a catalog with selected metadata return a comma separated string
    of the `data_key.field_name` for every selected field in the catalog plus
    the `extra_fields` defined below

    The result of this function is expected to be passed to the API as the
    value of the `fields` parameter

    We add '<data_key>' to the beginning of all the selected fields because
    fields are nested under the '<data_key>' key in the response object.
    """
    mdata = metadata.to_map(catalog.get_stream(stream_name).metadata)
    fields = catalog.get_stream(stream_name).schema.properties.keys()
    formatted_field_names = []
    for field in fields:
        field_metadata = mdata.get(('properties', field))
        if should_sync_field(field_metadata.get('inclusion'), field_metadata.get('selected')):
            formatted_field_names.append(data_key+'.'+field)

    default_fields = ['_links', 'total_items', 'constraints', data_key+'.'+'_links']
    formatted_field_names += default_fields
    if extra_fields:
        formatted_field_names += extra_fields
    return ",".join(formatted_field_names)



def should_sync_stream(streams_to_sync, dependants, stream_name):
    selected_streams = streams_to_sync['selected_streams']
    should_persist = stream_name in selected_streams
    last_stream = streams_to_sync['last_stream']
    if last_stream == stream_name or last_stream is None:
        if last_stream is not None:
            streams_to_sync['last_stream'] = None
            return True, should_persist
        if should_persist or set(dependants).intersection(selected_streams):
            return True, should_persist
    return False, should_persist

def chunk_campaigns(sorted_campaigns, chunk_bookmark):
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

def write_email_activity_chunk_bookmark(state, current_bookmark, current_index, sorted_campaigns):
    # Bookmark next chunk because the current chunk will be saved in batch_id
    # Index is relative to current bookmark
    next_chunk = current_bookmark + current_index + 1
    if next_chunk * EMAIL_ACTIVITY_BATCH_SIZE < len(sorted_campaigns):
        write_bookmark(state, ['reports_email_activity_next_chunk'], next_chunk)
    else:
        write_bookmark(state, ['reports_email_activity_next_chunk'], 0)

def check_and_resume_email_activity_batch(client, catalog, state, start_date):
    batch_id = get_bookmark(state, ['reports_email_activity_last_run_id'], None)

    if batch_id:
        try:
            data = get_batch_info(client, batch_id)
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
        sync_email_activity(client, catalog, state, start_date, campaigns, batch_id)

## TODO: is current_stream being updated?

def sync(client, catalog, state, start_date):
    streams_to_sync = {
        'selected_streams': get_selected_streams(catalog),
        'last_stream': state.get('current_stream')
    }

    if not streams_to_sync['selected_streams']:
        return

    id_bag = {}

    endpoints = {
        'lists': {
            'path': '/lists',
            'params': {
                'sort_field': 'date_created',
                'sort_dir': 'ASC'
            },
            'children': {
                'list_members': {
                    'path': '/lists/{}/members',
                    'data_path': 'members',
                    'bookmark_query_field': 'since_last_changed',
                    'bookmark_field': 'last_changed'
                },
                'list_segments': {
                    'path': '/lists/{}/segments',
                    'data_path': 'segments',
                    'children': {
                        'list_segment_members': {
                            'path': '/lists/{}/segments/{}/members',
                            'data_path': 'members'
                        }
                    }
                }
            }
        },
        'campaigns': {
            'dependants': [
                'reports_email_activity'
            ],
            'path': '/campaigns',
            'params': {
                'status': 'sent',
                'sort_field': 'send_time',
                'sort_dir': 'ASC'
            },
            'store_ids': True,
            'children': {
                'unsubscribes': {
                    'path': '/reports/{}/unsubscribed'
                }
            }
        },
        'automations': {
            'path': '/automations'
        }
    }

    for stream_name, endpoint_config in endpoints.items():
        sync_stream(client,
                    catalog,
                    state,
                    start_date,
                    streams_to_sync,
                    id_bag,
                    stream_name,
                    endpoint_config)

    should_stream, _ = should_sync_stream(streams_to_sync,
                                          [],
                                          'reports_email_activity')
    campaign_ids = id_bag.get('campaigns')
    if should_stream and campaign_ids:
        # Resume previous batch, if necessary
        check_and_resume_email_activity_batch(client, catalog, state, start_date)
        # Chunk batch_ids, bookmarking the chunk number
        sorted_campaigns = sorted(campaign_ids)
        chunk_bookmark = int(get_bookmark(state, ['reports_email_activity_next_chunk'], 0))
        for i, campaign_chunk in enumerate(chunk_campaigns(sorted_campaigns, chunk_bookmark)):
            write_email_activity_chunk_bookmark(state, chunk_bookmark, i, sorted_campaigns)
            sync_email_activity(client, catalog, state, start_date, campaign_chunk)

        # Start from the beginning next time
        write_bookmark(state, ['reports_email_activity_next_chunk'], 0)
