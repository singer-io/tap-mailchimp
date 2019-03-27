import re
import json
import time
import random
import tarfile
from datetime import datetime, timedelta

import requests
import singer
from singer import metrics, metadata, Transformer

LOGGER = singer.get_logger()

MIN_RETRY_INTERVAL = 2 # 2 seconds
MAX_RETRY_INTERVAL = 300 # 5 minutes
MAX_RETRY_ELAPSED_TIME = 3600 # 1 hour

def next_sleep_interval(previous_sleep_interval):
    min_interval = previous_sleep_interval or MIN_RETRY_INTERVAL
    max_interval = previous_sleep_interval * 2 or MIN_RETRY_INTERVAL
    return min(MAX_RETRY_INTERVAL, random.randint(min_interval, max_interval))

def write_schema(catalog, stream_id):
    stream = catalog.get_stream(stream_id)
    schema = stream.schema.to_dict()
    singer.write_schema(stream_id, schema, stream.key_properties)

def persist_records(catalog, stream_id, records):
    stream = catalog.get_stream(stream_id)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)
    with metrics.record_counter(stream_id) as counter:
        for record in records:
            with Transformer() as transformer:
                record = transformer.transform(record,
                                               schema,
                                               stream_metadata)
            singer.write_record(stream_id, record)
            counter.increment()

def get_bookmark(state, stream_name, default):
    return (
        state
        .get('bookmarks', {})
        .get(stream_name, default)
    )

def sync_endpoint(client,
                  catalog,
                  state,
                  start_date,
                  stream_name,
                  path,
                  data_key,
                  static_params,
                  bookmark_query_field=None,
                  bookmark_field=None):
    last_datetime = get_bookmark(state, stream_name, start_date)
    ids = []

    def transform(record):
        ids.append(record['id'])
        del record['_links']
        return record

    count = 1000
    offset = 0
    has_more = True
    while has_more:
        params = {
            'count': count,
            'offset': offset,
            **static_params
        }

        if bookmark_query_field:
            params[bookmark_query_field] = last_datetime

        data = client.get(
            path,
            params=params,
            endpoint=stream_name)

        raw_records = data.get(data_key)

        if len(raw_records) < count:
            has_more = False

        persist_records(catalog, stream_name, map(transform, raw_records))

    return ids

def get_email_activity_bookmark(state, campaign_id, default):
    return (
        state
        .get('bookmarks', {})
        .get('reports_email_activity', {})
        .get(campaign_id, default)
    )

def poll_email_activity(client, batch_id):
    sleep = 0
    start_time = time.time()
    while True:
        data = client.get(
            '/batches/{}'.format(batch_id),
            endpoint='poll_email_activity')

        LOGGER.info('reports_email_activity - Job polling: {} - {}'.format(
            data['id'],
            data['status']))

        if data['status'] == 'finished':
            return data
        elif (time.time() - start_time) > MAX_RETRY_ELAPSED_TIME:
            message = 'campaigns - export deadline exceeded ({} secs)'.format(
                MAX_RETRY_ELAPSED_TIME)
            LOGGER.error(message)
            raise Exception(message)

        sleep = next_sleep_interval(sleep)
        LOGGER.info('campaigns - status: {}, sleeping for {} seconds'.format(
                    data['status'],
                    sleep))
        time.sleep(sleep)

def stream_email_activity(client, catalog, state, archive_url):
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

    failed_campaign_ids = []
    with client.request('GET', url=archive_url, s3=True, endpoint='s3') as response:
        with tarfile.open(mode='r|gz', fileobj=response.raw) as tar:
            file = tar.next()
            while file:
                if file.isfile():
                    rawoperations = tar.extractfile(file)
                    operations = json.load(rawoperations)
                    for operation in operations:
                        campaign_id = operation['operation_id']
                        if operation['status_code'] != 200:
                            failed_campaign_ids.append(campaign_id)
                        else:
                            response = json.loads(operation['response'])
                            email_activities = response['emails']
                            persist_records(catalog,
                                            'reports_email_activity',
                                            transform_activities(email_activities))
                        ## TODO: update bookmark using campaign_id
                file = tar.next()
    return failed_campaign_ids

def sync_email_activity(client, catalog, state, start_date, campaign_ids):
    LOGGER.info('reports_email_activity - Starting sync')

    operations = []
    for campaign_id in campaign_ids:
        since = get_email_activity_bookmark(state, campaign_id, start_date)
        operations.append({
            'method': 'GET',
            'path': '/reports/{}/email-activity'.format(campaign_id),
            'operation_id': campaign_id,
            'params': {
                'since': since
            }
        })

    print(operations)

    data = client.post(
        '/batches',
        json={
            'operations': operations
        },
        endpoint='create_actvity_export')

    LOGGER.info('reports_email_activity - Job running: {}'.format(data['id']))

    ## TODO: update state

    data = poll_email_activity(client, data['id'])

    print(data)

    ## TODO: check num failed failed operations ??

    ## TODO: log completed_at - submitted_at diff

    failed_campaign_ids = stream_email_activity(client,
                                                catalog,
                                                state,
                                                data['response_body_url'])

    ## TODO: check num failed failed operations == num failed_campaign_ids ??

def get_selected_streams(catalog):
    selected_streams = set()
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        root_metadata = mdata.get(())
        if root_metadata and root_metadata.get('selected') is True:
            selected_streams.add(stream.tap_stream_id)
    return list(selected_streams)

def should_sync_stream(selected_streams, last_stream, stream_name):
    if last_stream == stream_name or last_stream is None:
        if last_stream is not None:
            last_stream = None
        if stream_name in selected_streams:
            return True, last_stream
    return False, last_stream

def sync(client, catalog, state, start_date):
    selected_streams = get_selected_streams(catalog)

    if not selected_streams:
        return

    last_stream = state.get('current_stream')

    should_stream, last_stream = should_sync_stream(selected_streams,
                                                    last_stream,
                                                    'campaigns')
    if should_stream:
        campaign_ids = sync_endpoint(client,
                                     catalog,
                                     state,
                                     start_date,
                                     'campaigns',
                                     '/campaigns',
                                     'campaigns',
                                     {
                                        'status': 'sent',
                                        'sort_field': 'send_time',
                                        'sort_dir': 'ASC'
                                     })

    # sync_email_activity(client, catalog, state, start_date, campaign_ids)

    # stream_email_activity(client, catalog, state, 'https://mailchimp-api-batch.s3.amazonaws.com/aeca9fbd30-response.tar.gz?AWSAccessKeyId=AKIAJO3NXSSIEMVRK7NQ&Expires=1553658340&Signature=mDK2%2BbHuAv1KCv3EFyWu%2BYWNp5Q%3D')
