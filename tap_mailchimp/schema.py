import os
import json
from singer import metadata

SCHEMAS = {}
FIELD_METADATA = {}

STREAMS = {
    'automations': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'campaigns': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'lists': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'list_members': {
        'key_properties': ['id', 'list_id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_changed'],
        'parent_stream': 'lists',
    },
    'list_segments': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'parent_stream': 'lists',
    },
    'list_segment_members': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'parent_stream': 'list_segments',
    },
    'reports_email_activity': {
        'key_properties': ['campaign_id', 'action', 'email_id', 'timestamp'],
        'replication_method': 'FULL_TABLE',
        'parent_stream': 'campaigns',
    },
    'unsubscribes': {
        'key_properties': ['campaign_id', 'email_id'],
        'replication_method': 'FULL_TABLE',
        'parent_stream': 'campaigns',
    },
}

def get_abs_path(path):
    """
    Get the absolute path for the schema files.
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas():
    """Prepare metadata for each stream and return schema and metadata for the catalog."""
    global SCHEMAS, FIELD_METADATA # pylint: disable=global-statement

    if SCHEMAS:
        return SCHEMAS, FIELD_METADATA

    for stream_name, stream_obj in STREAMS.items():
        schema_path = get_abs_path("schemas/{}.json".format(stream_name))
        with open(schema_path, encoding='UTF-8') as file:
            schema = json.load(file)

        SCHEMAS[stream_name] = schema

        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_obj.get("key_properties"),
            valid_replication_keys=(stream_obj.get("replication_keys") or []),
            replication_method=stream_obj.get("replication_method"),
        )
        mdata = metadata.to_map(mdata)

        automatic_keys = stream_obj.get("replication_keys") or []
        for field_name in schema["properties"].keys():
            if field_name in automatic_keys:
                mdata = metadata.write(
                    mdata, ("properties", field_name), "inclusion", "automatic"
                )

        parent_tap_stream_id = stream_obj.get("parent_stream")
        if parent_tap_stream_id:
            mdata = metadata.write(mdata, (), 'parent-tap-stream-id', parent_tap_stream_id)

        mdata = metadata.to_list(mdata)
        FIELD_METADATA[stream_name] = mdata

    return SCHEMAS, FIELD_METADATA
