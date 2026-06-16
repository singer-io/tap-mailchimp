import os
import json

SCHEMAS = {}
FIELD_METADATA = {}

PKS = {
    'automations': ['id'],
    'campaigns': ['id'],
    'list_members': ['id', 'list_id'],
    'list_segment_members': ['id'],
    'list_segments': ['id'],
    'lists': ['id'],
    'reports_email_activity': [
        'campaign_id',
        'action',
        'email_id',
        'timestamp'
    ],
    'unsubscribes': ['campaign_id', 'email_id']
}

REPLICATION_KEYS = {
    'list_members': ['last_changed'],
}

REPLICATION_METHODS = {
    'automations': 'FULL_TABLE',
    'campaigns': 'FULL_TABLE',
    'list_segment_members': 'FULL_TABLE',
    'list_segments': 'FULL_TABLE',
    'lists': 'FULL_TABLE',
    'unsubscribes': 'FULL_TABLE',
    'list_members': 'INCREMENTAL',
    'reports_email_activity': 'FULL_TABLE',
}

PARENT_STREAMS = {
    'list_members': 'lists',
    'list_segments': 'lists',
    'list_segment_members': 'list_segments',
    'unsubscribes': 'campaigns',
    'reports_email_activity': 'campaigns',
}

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas():
    global SCHEMAS, FIELD_METADATA # pylint: disable=global-statement

    if SCHEMAS:
        return SCHEMAS, FIELD_METADATA

    schemas_path = get_abs_path('schemas')

    file_names = [f for f in os.listdir(schemas_path)
                  if os.path.isfile(os.path.join(schemas_path, f))]

    for file_name in file_names:
        stream_name = file_name[:-5]
        with open(os.path.join(schemas_path, file_name), encoding='UTF-8') as data_file:
            schema = json.load(data_file)

        SCHEMAS[stream_name] = schema
        pk = PKS[stream_name]
        replication_keys = REPLICATION_KEYS.get(stream_name, [])

        metadata = []

        # Root-level stream metadata
        root_metadata = {}
        replication_method = REPLICATION_METHODS.get(stream_name)
        if replication_method:
            root_metadata['forced-replication-method'] = replication_method
        if replication_keys:
            root_metadata['valid-replication-keys'] = replication_keys
        parent_stream = PARENT_STREAMS.get(stream_name)
        if parent_stream:
            root_metadata['parent-tap-stream-id'] = parent_stream
        if root_metadata:
            metadata.append({
                'metadata': root_metadata,
                'breadcrumb': []
            })

        for prop in schema['properties'].keys():
            if prop in pk or prop in replication_keys:
                inclusion = 'automatic'
            else:
                inclusion = 'available'
            metadata.append({
                'metadata': {
                    'inclusion': inclusion
                },
                'breadcrumb': ['properties', prop]
            })
        FIELD_METADATA[stream_name] = metadata

    return SCHEMAS, FIELD_METADATA
