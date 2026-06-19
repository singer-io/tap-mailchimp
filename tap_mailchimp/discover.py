import copy
import singer
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_mailchimp.schema import get_schemas, STREAMS
from tap_mailchimp.client import MailchimpForbiddenError

LOGGER = singer.get_logger()

# Parent streams are probed directly for read access during discovery.
PARENT_STREAM_PATHS = {
    'automations': '/automations',
    'campaigns': '/campaigns',
    'lists': '/lists',
}

# Child streams inherit access from their parent stream.
CHILD_PARENT_MAP = {
    stream_name: stream_obj.get('parent_stream')
    for stream_name, stream_obj in STREAMS.items()
    if stream_obj.get('parent_stream')
}

def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """
    Probe each parent stream for read access and remove inaccessible streams
    (and their children) from schemas and field_metadata in place.
    """
    inaccessible_streams = []

    for stream_name, endpoint_path in PARENT_STREAM_PATHS.items():
        if stream_name not in schemas:
            continue

        try:
            client.get(endpoint_path, params={'count': 1}, endpoint=stream_name)
        except MailchimpForbiddenError as exc:
            LOGGER.warning(
                "Excluding unauthorized stream '%s' from catalog. HTTP-Error-Message: '%s'",
                stream_name,
                str(exc),
            )
            inaccessible_streams.append(stream_name)

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    _prune_inaccessible_children(schemas, field_metadata)

    if not schemas:
        raise MailchimpForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have 'read' access to any supported streams."
        )

    if inaccessible_streams:
        LOGGER.warning(
            "No 'read' access to stream(s): %s. Excluded from catalog.",
            ', '.join(inaccessible_streams),
        )


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """
    Remove child streams from the catalog whose parent stream was excluded.
    Mutates schemas and field_metadata in place.
    """
    removed_stream = True
    while removed_stream:
        removed_stream = False
        for stream_name, parent_stream in CHILD_PARENT_MAP.items():
            if stream_name in schemas and parent_stream not in schemas:
                LOGGER.warning(
                    "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                    stream_name,
                    parent_stream,
                )
                schemas.pop(stream_name, None)
                field_metadata.pop(stream_name, None)
                removed_stream = True


def discover(client) -> Catalog:
    """
    Run discovery and return a catalog.
    Streams the credentials cannot access are excluded from the returned catalog.
    """
    schemas, field_metadata = get_schemas()

    # Avoid mutating the module-level schema/metadata cache.
    schemas = copy.copy(schemas)
    field_metadata = copy.copy(field_metadata)

    _apply_access_checks(client, schemas, field_metadata)

    catalog = Catalog([])

    for stream_name, schema in schemas.items():
        stream_obj = STREAMS[stream_name]

        catalog.streams.append(
            CatalogEntry(
                tap_stream_id=stream_name,
                stream=stream_name,
                schema=Schema.from_dict(schema),
                metadata=field_metadata[stream_name],
                key_properties=stream_obj.get('key_properties', []),
                replication_key=(stream_obj.get('replication_keys') or [None])[0],
                replication_method=stream_obj.get('replication_method'),
            )
        )

    return catalog
