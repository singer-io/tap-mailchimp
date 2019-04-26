from singer.catalog import Catalog, CatalogEntry, Schema

from tap_mailchimp.schema import get_schemas, PKS

def discover():
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        metadata = field_metadata[stream_name]
        pk = PKS[stream_name]

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=pk,
            schema=schema,
            metadata=metadata
        ))

    return catalog
