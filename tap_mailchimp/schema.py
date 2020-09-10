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
        with open(os.path.join(schemas_path, file_name)) as data_file:
            schema = json.load(data_file)

        SCHEMAS[stream_name] = schema
        pk = PKS[stream_name]
        replication_keys = REPLICATION_KEYS.get(stream_name, [])

        metadata = []
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
