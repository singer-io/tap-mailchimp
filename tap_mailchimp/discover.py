from singer.catalog import Catalog, CatalogEntry, Schema
from tap_mailchimp.streams import STREAMS

def discover():
    catalog = Catalog([])

    for _, stream_obj in STREAMS.items():
        schemas, field_metadata = stream_obj.get_schema()

        # loop over the schema and prepare catalog
        for stream_name, schema_dict in schemas.items():

            schema = Schema.from_dict(schema_dict)
            metadata = field_metadata[stream_name]

            key_props = None
            # Get the primary key for the stream
            for mdata_entry in metadata:
                table_key_properties = mdata_entry.get('metadata', {}).get('table-key-properties')
                if table_key_properties:
                    key_props = table_key_properties

            catalog.streams.append(CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_props,
                schema=schema,
                metadata=metadata
            ))

    return catalog
