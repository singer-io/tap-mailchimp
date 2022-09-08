from singer.catalog import Catalog, CatalogEntry, Schema
from tap_mailchimp.schema import get_schemas
from tap_mailchimp.streams import STREAMS

def discover():
    """Prepare catalog for all the streams"""
    catalog = Catalog([])
    # Get schemas and metadata of all streams
    schemas, metadatas = get_schemas()

    # Prepare catalog
    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=STREAMS[stream_name].key_properties,
            schema=schema,
            metadata=metadatas[stream_name]
        ))

    return catalog
