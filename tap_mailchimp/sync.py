import singer
from tap_mailchimp.streams import STREAMS

LOGGER = singer.get_logger()

def get_streams_to_sync(catalog, selected_streams, selected_stream_names):
    streams_to_sync = []
    child_streams_to_sync = []
    for stream in selected_streams:
        parent_streams = STREAMS.get(stream.tap_stream_id).streams_to_sync
        if parent_streams:
            if parent_streams[0] not in selected_stream_names:
                streams_to_sync.append(catalog.get_stream(parent_streams[0]))
            child_streams_to_sync += list(set(parent_streams[1:]))
        else:
            streams_to_sync.append(stream)
    return streams_to_sync, child_streams_to_sync

# Function for sync mode
def sync(client, catalog, state, config):

    selected_streams = list(catalog.get_selected_streams(state))
    selected_stream_names = []
    for stream in selected_streams:
        selected_stream_names.append(stream.stream)

    for stream_name, stream_obj in STREAMS.items():
        if stream_name in selected_stream_names:
            stream_obj.write_schema(catalog)

    streams_to_sync, child_streams_to_sync = get_streams_to_sync(catalog, selected_streams, selected_stream_names)
    for stream in streams_to_sync:
        stream_id = stream.tap_stream_id
        stream_object = STREAMS.get(stream_id)(state, client, config, catalog, selected_stream_names, child_streams_to_sync)

        LOGGER.info('START Syncing: {}'.format(stream_id))

        # Set currently syncing stream
        state = singer.set_currently_syncing(state, stream_id)
        singer.write_state(state)

        stream_object.sync()

        LOGGER.info('FINISHED Syncing: {}'.format(stream_id))

    # remove currently_syncing at the end of the sync
    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
