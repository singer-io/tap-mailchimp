import json
import os
from singer import metadata
from tap_mailchimp.streams import STREAMS


def get_abs_path(path):
    """Function to get the path of the stream schema file"""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schemas():
    """Function to generate catalog by fetching schema from the """
    schemas = {}
    field_metadata = {}

    for stream_obj in STREAMS.values():
        stream_name = stream_obj.stream_name
        schema_path = get_abs_path('schemas/{}.json'.format(stream_name))

        with open(schema_path) as file:
            schema_dict = json.load(file)
        schemas[stream_name] = schema_dict

        # Documentation:
        #   https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#singer-python-helper-functions
        # Reference:
        #   https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
        mdata = metadata.get_standard_metadata(
            schema=schema_dict,
            key_properties=stream_obj.key_properties,
            valid_replication_keys=stream_obj.replication_keys,
            replication_method=stream_obj.replication_method
        )

        mdata_map = metadata.to_map(mdata)

        # Update inclusion of "replication keys" as "automatic"
        if stream_obj.replication_keys:
            for replication_key in stream_obj.replication_keys:
                mdata_map[('properties', replication_key)]['inclusion'] = 'automatic'

        # Update inclusion for extra fields which we need to replicate data
        if stream_obj.extra_automatic_fields:
            for field in stream_obj.extra_automatic_fields:
                mdata_map[('properties', field)]['inclusion'] = 'automatic'

        metadata_list = metadata.to_list(mdata_map)
        field_metadata[stream_name] = metadata_list

    return schemas, field_metadata
