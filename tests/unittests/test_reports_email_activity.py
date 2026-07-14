import io
import json
import tarfile
import unittest
from unittest.mock import MagicMock, patch, call

import singer

from tap_mailchimp.sync import (
    get_bookmark,
    process_records,
    stream_email_activity,
    write_bookmark,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_catalog(stream_name='reports_email_activity'):
    """Return a minimal singer.Catalog for reports_email_activity."""
    return singer.Catalog.from_dict({
        'streams': [{
            'tap_stream_id': stream_name,
            'stream': stream_name,
            'key_properties': ['campaign_id', 'action', 'email_id', 'timestamp'],
            'schema': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'campaign_id': {'type': ['string']},
                    'list_id':     {'type': ['string']},
                    'list_is_active': {'type': ['boolean']},
                    'email_id':    {'type': ['null', 'string']},
                    'email_address': {'type': ['null', 'string']},
                    'action':      {'type': ['null', 'string']},
                    'type':        {'type': ['null', 'string']},
                    'timestamp':   {'type': ['string'], 'format': 'date-time'},
                    'ip':          {'type': ['null', 'string']},
                    'url':         {'type': ['null', 'string']},
                },
            },
            'metadata': [
                {'breadcrumb': [],
                 'metadata': {'selected': True, 'inclusion': 'available'}},
                {'breadcrumb': ['properties', 'campaign_id'],
                 'metadata': {'inclusion': 'automatic'}},
                {'breadcrumb': ['properties', 'email_id'],
                 'metadata': {'inclusion': 'automatic'}},
                {'breadcrumb': ['properties', 'action'],
                 'metadata': {'inclusion': 'automatic'}},
                {'breadcrumb': ['properties', 'timestamp'],
                 'metadata': {'inclusion': 'automatic'}},
                {'breadcrumb': ['properties', 'list_id'],
                 'metadata': {'inclusion': 'available', 'selected': True}},
                {'breadcrumb': ['properties', 'list_is_active'],
                 'metadata': {'inclusion': 'available', 'selected': True}},
                {'breadcrumb': ['properties', 'email_address'],
                 'metadata': {'inclusion': 'available', 'selected': True}},
                {'breadcrumb': ['properties', 'type'],
                 'metadata': {'inclusion': 'available', 'selected': True}},
                {'breadcrumb': ['properties', 'ip'],
                 'metadata': {'inclusion': 'available', 'selected': True}},
                {'breadcrumb': ['properties', 'url'],
                 'metadata': {'inclusion': 'available', 'selected': True}},
            ],
        }]
    })


def _make_tar_gz_bytes(operations):
    """
    Build an in-memory .tar.gz archive containing a single JSON file
    whose content is the *list* of batch operations supplied.
    """
    json_bytes = json.dumps(operations).encode('utf-8')

    buf = io.BytesIO()
    with tarfile.open(mode='w:gz', fileobj=buf) as tar:
        info = tarfile.TarInfo(name='operations.json')
        info.size = len(json_bytes)
        tar.addfile(info, io.BytesIO(json_bytes))
    buf.seek(0)
    return buf.read()


def _make_client_response(operations):
    """Return a mock client whose GET (s3=True) streams the tar.gz bytes."""
    raw = io.BytesIO(_make_tar_gz_bytes(operations))
    raw.read = raw.read  # already a real BytesIO

    response_mock = MagicMock()
    response_mock.raw = raw
    response_mock.__enter__ = lambda s: s
    response_mock.__exit__ = MagicMock(return_value=False)

    client = MagicMock()
    client.request.return_value = response_mock
    return client


# ---------------------------------------------------------------------------
# Unit tests: get_bookmark (null-fallback behaviour)
# ---------------------------------------------------------------------------

class TestGetBookmark(unittest.TestCase):
    """get_bookmark should fall back to *default* when a stored value is None."""

    def test_missing_key_returns_default(self):
        state = {}
        self.assertEqual(
            get_bookmark(state, ['reports_email_activity', 'cid1'], '2020-01-01'),
            '2020-01-01',
        )

    def test_existing_value_returned(self):
        state = {'bookmarks': {'reports_email_activity': {'cid1': '2023-06-01T00:00:00Z'}}}
        self.assertEqual(
            get_bookmark(state, ['reports_email_activity', 'cid1'], '2020-01-01'),
            '2023-06-01T00:00:00Z',
        )

    def test_stored_null_falls_back_to_default(self):
        """
        A null written to state (e.g. from an empty first-sync response) must
        NOT be returned as-is; it should fall back to start_date.
        """
        state = {'bookmarks': {'reports_email_activity': {'cid_empty': None}}}
        result = get_bookmark(state, ['reports_email_activity', 'cid_empty'], '2020-01-01')
        self.assertEqual(result, '2020-01-01')

    def test_stored_null_does_not_become_since_none(self):
        """
        Verify that passing the result of get_bookmark as `since` param won't
        produce None (which would cause the API to return all records).
        """
        state = {'bookmarks': {'reports_email_activity': {'cid_empty': None}}}
        since = get_bookmark(state, ['reports_email_activity', 'cid_empty'], '2019-01-01T00:00:00Z')
        self.assertIsNotNone(since)


# ---------------------------------------------------------------------------
# Unit tests: process_records bookmark tracking
# ---------------------------------------------------------------------------

class TestProcessRecordsBookmark(unittest.TestCase):
    """process_records should track the max timestamp across records."""

    def setUp(self):
        self.catalog = _make_catalog()

    @patch('singer.write_record')
    @patch('singer.write_schema')
    def test_returns_max_timestamp(self, _ws, _wr):
        records = [
            {'campaign_id': 'c1', 'email_id': 'e1', 'action': 'open',
             'timestamp': '2023-01-01T10:00:00Z', 'list_id': 'l1',
             'list_is_active': True},
            {'campaign_id': 'c1', 'email_id': 'e2', 'action': 'click',
             'timestamp': '2023-01-03T12:00:00Z', 'list_id': 'l1',
             'list_is_active': True},
            {'campaign_id': 'c1', 'email_id': 'e3', 'action': 'open',
             'timestamp': '2023-01-02T08:00:00Z', 'list_id': 'l1',
             'list_is_active': True},
        ]
        result = process_records(
            self.catalog,
            'reports_email_activity',
            iter(records),
            bookmark_field='timestamp',
            max_bookmark_field=None,
        )
        self.assertEqual(result, '2023-01-03T12:00:00Z')

    @patch('singer.write_record')
    @patch('singer.write_schema')
    def test_empty_records_returns_none(self, _ws, _wr):
        """Empty response — max_bookmark_field stays None (no bookmark advance)."""
        result = process_records(
            self.catalog,
            'reports_email_activity',
            iter([]),
            bookmark_field='timestamp',
            max_bookmark_field=None,
        )
        self.assertIsNone(result)

    @patch('singer.write_record')
    @patch('singer.write_schema')
    def test_existing_bookmark_not_regressed(self, _ws, _wr):
        """
        When records are older than the existing bookmark, the existing
        bookmark value must be returned unchanged.
        """
        existing_bookmark = '2023-06-01T00:00:00Z'
        records = [
            {'campaign_id': 'c1', 'email_id': 'e1', 'action': 'open',
             'timestamp': '2023-01-01T10:00:00Z', 'list_id': 'l1',
             'list_is_active': True},
        ]
        result = process_records(
            self.catalog,
            'reports_email_activity',
            iter(records),
            bookmark_field='timestamp',
            max_bookmark_field=existing_bookmark,
        )
        self.assertEqual(result, existing_bookmark)


# ---------------------------------------------------------------------------
# Unit tests: stream_email_activity bookmark write behaviour
# ---------------------------------------------------------------------------

class TestStreamEmailActivityBookmarks(unittest.TestCase):
    """
    Verify bookmark write logic inside stream_email_activity:
    - advances bookmark when records present
    - does NOT write bookmark (or write null) when batch returns empty emails
    - does NOT overwrite a valid existing bookmark with null on empty response
    """

    def _run(self, operations, initial_state=None):
        """Helper: run stream_email_activity and return final state."""
        state = initial_state or {}
        catalog = _make_catalog()
        client = _make_client_response(operations)

        with patch('singer.write_schema'), \
             patch('singer.write_record'), \
             patch('singer.write_state'):
            stream_email_activity(client, catalog, state, archive_url='http://fake-s3')

        return state

    def test_bookmark_advances_after_records(self):
        operations = [{
            'operation_id': 'cid1',
            'status_code': 200,
            'response': json.dumps({
                'emails': [{
                    'campaign_id': 'cid1',
                    'email_id': 'eid1',
                    'list_id': 'lid1',
                    'list_is_active': True,
                    'activity': [
                        {'action': 'open', 'timestamp': '2023-03-01T10:00:00Z'},
                        {'action': 'click', 'timestamp': '2023-03-02T12:00:00Z'},
                    ],
                }],
            }),
        }]
        state = self._run(operations)
        bookmark = state.get('bookmarks', {}).get('reports_email_activity', {}).get('cid1')
        self.assertEqual(bookmark, '2023-03-02T12:00:00Z')

    def test_empty_emails_does_not_write_null_bookmark(self):
        """
        When the API returns an empty emails array for a campaign's first sync,
        the bookmark must NOT be written as null.
        """
        operations = [{
            'operation_id': 'cid_new',
            'status_code': 200,
            'response': json.dumps({'emails': []}),
        }]
        state = self._run(operations)
        bookmarks = state.get('bookmarks', {}).get('reports_email_activity', {})
        self.assertNotIn('cid_new', bookmarks,
                         "Bookmark must not be written at all for an empty first-sync response")

    def test_empty_emails_preserves_existing_bookmark(self):
        """
        When a subsequent sync returns no new records, the existing bookmark
        must be kept intact.
        """
        initial_state = {
            'bookmarks': {
                'reports_email_activity': {'cid_existing': '2023-01-15T00:00:00Z'}
            }
        }
        operations = [{
            'operation_id': 'cid_existing',
            'status_code': 200,
            'response': json.dumps({'emails': []}),
        }]
        state = self._run(operations, initial_state=initial_state)
        bookmark = state['bookmarks']['reports_email_activity']['cid_existing']
        self.assertEqual(bookmark, '2023-01-15T00:00:00Z',
                         "Existing bookmark must not be overwritten by an empty response")

    def test_failed_operation_not_bookmarked(self):
        operations = [{
            'operation_id': 'cid_fail',
            'status_code': 400,
            'response': json.dumps({'detail': 'Bad request'}),
        }]
        state = self._run(operations)
        bookmarks = state.get('bookmarks', {}).get('reports_email_activity', {})
        self.assertNotIn('cid_fail', bookmarks)

    def test_mixed_operations_bookmark_only_successes(self):
        operations = [
            {
                'operation_id': 'cid_ok',
                'status_code': 200,
                'response': json.dumps({
                    'emails': [{
                        'campaign_id': 'cid_ok',
                        'email_id': 'eid1',
                        'list_id': 'lid1',
                        'list_is_active': True,
                        'activity': [
                            {'action': 'open', 'timestamp': '2023-05-01T09:00:00Z'},
                        ],
                    }],
                }),
            },
            {
                'operation_id': 'cid_fail',
                'status_code': 400,
                'response': json.dumps({'detail': 'Bad request'}),
            },
        ]
        state = self._run(operations)
        bookmarks = state.get('bookmarks', {}).get('reports_email_activity', {})
        self.assertIn('cid_ok', bookmarks)
        self.assertNotIn('cid_fail', bookmarks)


# ---------------------------------------------------------------------------
# Unit tests: schema & catalog declarations
# ---------------------------------------------------------------------------

class TestSchemaDeclarations(unittest.TestCase):
    """Verify schema.py declares reports_email_activity as INCREMENTAL."""

    def test_replication_method_is_incremental(self):
        from tap_mailchimp.schema import STREAMS
        stream = STREAMS['reports_email_activity']
        self.assertEqual(stream.get('replication_method'), 'INCREMENTAL')

    def test_replication_key_is_timestamp(self):
        from tap_mailchimp.schema import STREAMS
        stream = STREAMS['reports_email_activity']
        self.assertIn('timestamp', stream.get('replication_keys', []))
