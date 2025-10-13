from singer.catalog import Catalog, CatalogEntry, Schema

from tap_mailchimp.schema import get_schemas, PKS, REPLICATION_METHODS

class CustomCatalogEntry(CatalogEntry):

    def __init__(self, *args, forced_replication_method=None, **kwargs):
        self.forced_replication_method = kwargs.pop('forced_replication_method', forced_replication_method)
        super().__init__(*args, **kwargs)

    def to_dict(self):
        result = super().to_dict()
        if self.forced_replication_method is not None:
            result['forced_replication_method'] = self.forced_replication_method
        return result

def discover():
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        metadata = field_metadata[stream_name]
        pk = PKS[stream_name]

        catalog.streams.append(CustomCatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=pk,
            forced_replication_method=REPLICATION_METHODS[stream_name],
            schema=schema,
            metadata=metadata
        ))

    return catalog
