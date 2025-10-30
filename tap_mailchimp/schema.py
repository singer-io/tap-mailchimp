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

ENDPOINTS = {
    'lists': {
        'path': '/lists',
        'replication_method': 'FULL_TABLE',
        'replication_keys': [],
        'key_properties': ['id'],
        'params': {
            'sort_field': 'date_created',
            'sort_dir': 'ASC'
        },
        'children': {
            'list_members': {
                'path': '/lists/{}/members',
                'replication_method': 'INCREMENTAL',
                'replication_keys': ['last_changed'],
                'key_properties': ['id', 'list_id'],
                'data_path': 'members',
                'parent': 'lists',
                'bookmark_query_field': 'since_last_changed',
                'bookmark_field': 'last_changed'
            },
            'list_segments': {
                'path': '/lists/{}/segments',
                'replication_method': 'FULL_TABLE',
                'replication_keys': [],
                'key_properties': ['id'],
                'data_path': 'segments',
                'parent': 'lists',
                'children': {
                    'list_segment_members': {
                        'path': '/lists/{}/segments/{}/members',
                        'replication_method': 'FULL_TABLE',
                        'replication_keys': [],
                        'key_properties': ['id'],
                        'parent': 'segments',
                        'data_path': 'members'
                    }
                }
            }
        }
    },
    'campaigns': {
        'dependants': ['reports_email_activity'],
        'path': '/campaigns',
        'replication_method': 'FULL_TABLE',
        'replication_keys': [],
        'key_properties': ['id'],
        'params': {
            'status': 'sent',
            'sort_field': 'send_time',
            'sort_dir': 'ASC'
        },
        'store_ids': True,
        'children': {
            'unsubscribes': {
                'path': '/reports/{}/unsubscribed',
                'replication_method': 'FULL_TABLE',
                'replication_keys': [],
                'key_properties': ['campaign_id', 'email_id'],
                'parent': 'campaigns'
            }
        }
    },
    'automations': {
        'path': '/automations',
        'replication_method': 'FULL_TABLE',
        'replication_keys': [],
        'key_properties': ['id']
    }
}

def find_stream_config(stream_name, endpoints=None):
    if endpoints is None:
        endpoints = ENDPOINTS
    for name, config in endpoints.items():
        if name == stream_name:
            return config
        if 'children' in config:
            found = find_stream_config(stream_name, config['children'])
            if found:
                return found
    return None

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas():
    global SCHEMAS, FIELD_METADATA

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

        stream_config = find_stream_config(stream_name)
        if stream_config is None:
            pk = PKS.get(stream_name, [])
            replication_keys = []
            replication_method = "FULL_TABLE"
            parent = 'campaigns' if stream_name == 'reports_email_activity' else None
        else:
            pk = stream_config.get('key_properties', [])
            replication_keys = stream_config.get('replication_keys', [])
            replication_method = stream_config.get('replication_method', "FULL_TABLE")
            parent = stream_config.get('parent', None)
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
        metadata.append({
            'metadata': {
                'inclusion': 'available',
                'forced-replication-method': replication_method,
                'valid-replication-keys': replication_keys,
                **({'parent-tap-stream-id': parent} if parent else {})
            },
            'breadcrumb': []
        })

        FIELD_METADATA[stream_name] = metadata

    return SCHEMAS, FIELD_METADATA