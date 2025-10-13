from singer.catalog import Catalog, CatalogEntry, Schema

from tap_mailchimp.schema import get_schemas, PKS, REPLICATION_METHODS, CHILD_STREAMS


class CustomCatalogEntry(CatalogEntry):

    def __init__(self, *args, forced_replication_method=None,parent_stream_id=None, **kwargs):
        self.forced_replication_method = kwargs.pop('forced-replication-method', forced_replication_method)
        self.parent_stream_id = kwargs.pop('parent-stream-id', parent_stream_id)

        super().__init__(*args, **kwargs)

    def to_dict(self):
        result = super().to_dict()
        if self.forced_replication_method is not None:
            result['forced-replication-method'] = self.forced_replication_method
        if self.parent_stream_id is not None:
            result['parent-stream-id'] = self.parent_stream_id
        return result

def discover():
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        metadata = field_metadata[stream_name]
        pk = PKS[stream_name]
        parent_stream_id = CHILD_STREAMS.get(stream_name, None)

        catalog.streams.append(CustomCatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=pk,
            forced_replication_method=REPLICATION_METHODS[stream_name],
            **({'parent-stream-id': CHILD_STREAMS[stream_name]} if parent_stream_id else {}),
            schema=schema,
            metadata=metadata
        ))

    return catalog
