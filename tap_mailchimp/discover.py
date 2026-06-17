import singer
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_mailchimp.schema import get_schemas, STREAMS
from tap_mailchimp.client import MailchimpForbiddenError

LOGGER = singer.get_logger()

# API paths used to probe read access for each parent stream
PARENT_STREAM_PATHS = {
    'automations': '/automations',
    'campaigns': '/campaigns',
    'lists': '/lists',
}

# Maps each child stream to its direct parent stream
CHILD_PARENT_MAP = {
    'list_members': 'lists',
    'list_segments': 'lists',
    'list_segment_members': 'list_segments',
    'unsubscribes': 'campaigns',
    'reports_email_activity': 'campaigns',
}


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """
    Probe each parent stream for read access and remove inaccessible streams
    (and their children) from schemas and field_metadata in place.
    Raises MailchimpForbiddenError if no parent streams are accessible.
    """
    inaccessible_streams = []
    for stream_name, path in PARENT_STREAM_PATHS.items():
        if stream_name not in schemas:
            continue
        try:
            client.get(path, params={'count': 1}, endpoint=stream_name)
        except MailchimpForbiddenError:
            LOGGER.warning(
                "Stream '%s' does not have read permission, excluding from catalog.",
                stream_name,
            )
            inaccessible_streams.append(stream_name)

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    _prune_inaccessible_children(schemas, field_metadata)

    if inaccessible_streams and len(inaccessible_streams) == len(PARENT_STREAM_PATHS):
        raise MailchimpForbiddenError(
            "HTTP-error-code: 403, Error: The account credentials supplied do not have 'read' "
            "access to any of the streams supported by the tap. Data collection cannot be "
            "initiated due to lack of permissions."
        )

    if inaccessible_streams:
        LOGGER.warning(
            "The account credentials supplied do not have 'read' access to the following "
            "stream(s): %s. These streams have been excluded from the catalog.",
            ", ".join(inaccessible_streams),
        )


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """
    Remove child streams from the catalog whose parent stream was excluded.
    Mutates schemas and field_metadata in place.
    """
    for child_name, parent_name in CHILD_PARENT_MAP.items():
        if child_name in schemas and parent_name not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                child_name,
                parent_name,
            )
            schemas.pop(child_name)
            field_metadata.pop(child_name)


def discover(client):
    schemas, field_metadata = get_schemas()
    # Copy to avoid mutating the module-level cache
    schemas = dict(schemas)
    field_metadata = dict(field_metadata)

    _apply_access_checks(client, schemas, field_metadata)

    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        metadata = field_metadata[stream_name]
        pk = STREAMS[stream_name]['key_properties']

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=pk,
            schema=schema,
            metadata=metadata
        ))

    return catalog
