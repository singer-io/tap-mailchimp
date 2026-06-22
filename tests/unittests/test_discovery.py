import unittest
from unittest.mock import MagicMock, patch

from tap_mailchimp.client import MailchimpForbiddenError
from tap_mailchimp.__init__ import do_discover
from tap_mailchimp.discover import (
    CHILD_PARENT_MAP,
    PARENT_STREAM_PATHS,
    _apply_access_checks,
    _prune_inaccessible_children,
    discover,
)


ALL_STREAM_NAMES = {
    'automations', 'campaigns', 'lists',
    'list_members', 'list_segments', 'list_segment_members',
    'unsubscribes', 'reports_email_activity',
}


def _make_schemas():
    """Return a fresh copy of minimal schemas and field_metadata dicts."""
    schemas = {name: {'properties': {}} for name in ALL_STREAM_NAMES}
    field_metadata = {name: [] for name in ALL_STREAM_NAMES}
    return schemas, field_metadata


def _make_client(forbidden_streams=None):
    """Return a mock MailchimpClient. Streams listed in forbidden_streams raise 403."""
    forbidden_streams = forbidden_streams or []
    client = MagicMock()

    def _get(path, **kwargs):
        endpoint = kwargs.get('endpoint', '')
        if endpoint in forbidden_streams:
            raise MailchimpForbiddenError(f'HTTP-error-code: 403, Error: Forbidden ({endpoint})')

    client.get.side_effect = _get
    return client


class TestApplyAccessChecks(unittest.TestCase):
    def test_all_streams_accessible(self):
        """No streams removed when all parent streams return 200."""
        client = _make_client(forbidden_streams=[])
        schemas, field_metadata = _make_schemas()

        _apply_access_checks(client, schemas, field_metadata)

        self.assertEqual(set(schemas.keys()), ALL_STREAM_NAMES)

    def test_partial_access_removes_inaccessible_parent(self):
        """Inaccessible parent is removed; accessible parents and unrelated children stay."""
        client = _make_client(forbidden_streams=['automations'])
        schemas, field_metadata = _make_schemas()

        _apply_access_checks(client, schemas, field_metadata)

        self.assertNotIn('automations', schemas)
        self.assertIn('campaigns', schemas)
        self.assertIn('lists', schemas)

    def test_inaccessible_parent_removes_its_children(self):
        """When 'lists' is forbidden, list_members, list_segments, list_segment_members are removed."""
        client = _make_client(forbidden_streams=['lists'])
        schemas, field_metadata = _make_schemas()

        _apply_access_checks(client, schemas, field_metadata)

        self.assertNotIn('lists', schemas)
        self.assertNotIn('list_members', schemas)
        self.assertNotIn('list_segments', schemas)
        self.assertNotIn('list_segment_members', schemas)
        # Unrelated streams survive
        self.assertIn('campaigns', schemas)
        self.assertIn('automations', schemas)

    def test_inaccessible_campaigns_removes_children(self):
        """When 'campaigns' is forbidden, unsubscribes and reports_email_activity are removed."""
        client = _make_client(forbidden_streams=['campaigns'])
        schemas, field_metadata = _make_schemas()

        _apply_access_checks(client, schemas, field_metadata)

        self.assertNotIn('campaigns', schemas)
        self.assertNotIn('unsubscribes', schemas)
        self.assertNotIn('reports_email_activity', schemas)
        self.assertIn('lists', schemas)
        self.assertIn('automations', schemas)

    def test_all_parents_inaccessible_raises(self):
        """Raises MailchimpForbiddenError when every parent stream is forbidden."""
        client = _make_client(forbidden_streams=list(PARENT_STREAM_PATHS.keys()))
        schemas, field_metadata = _make_schemas()

        with self.assertRaises(MailchimpForbiddenError):
            _apply_access_checks(client, schemas, field_metadata)

    def test_field_metadata_pruned_with_schemas(self):
        """field_metadata entries are removed alongside their schema counterparts."""
        client = _make_client(forbidden_streams=['automations'])
        schemas, field_metadata = _make_schemas()

        _apply_access_checks(client, schemas, field_metadata)

        self.assertNotIn('automations', field_metadata)

    def test_access_check_calls_correct_endpoints(self):
        """client.get is called once per parent stream with count=1."""
        client = _make_client()
        schemas, field_metadata = _make_schemas()

        _apply_access_checks(client, schemas, field_metadata)

        called_endpoints = {call.kwargs.get('endpoint') for call in client.get.call_args_list}
        self.assertEqual(called_endpoints, set(PARENT_STREAM_PATHS.keys()))

        for call in client.get.call_args_list:
            self.assertEqual(call.kwargs.get('params'), {'count': 1})


class TestPruneInaccessibleChildren(unittest.TestCase):
    def test_child_removed_when_parent_absent(self):
        schemas = {'list_members': {}}
        field_metadata = {'list_members': []}

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn('list_members', schemas)
        self.assertNotIn('list_members', field_metadata)

    def test_grandchild_removed_when_parent_absent(self):
        """list_segment_members removed when list_segments (its parent) is absent."""
        schemas = {'list_segment_members': {}}
        field_metadata = {'list_segment_members': []}

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn('list_segment_members', schemas)

    def test_child_kept_when_parent_present(self):
        schemas = {'lists': {}, 'list_members': {}}
        field_metadata = {'lists': [], 'list_members': []}

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertIn('list_members', schemas)

    def test_unrelated_streams_untouched(self):
        schemas = {'automations': {}, 'campaigns': {}}
        field_metadata = {'automations': [], 'campaigns': []}

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertIn('automations', schemas)
        self.assertIn('campaigns', schemas)


class TestDiscover(unittest.TestCase):
    @patch('tap_mailchimp.discover.get_schemas')
    def test_discover_returns_catalog_with_all_streams(self, mock_get_schemas):
        """When all streams accessible, catalog contains all streams."""
        mock_schemas = {name: {'properties': {}} for name in ALL_STREAM_NAMES}
        mock_metadata = {name: [] for name in ALL_STREAM_NAMES}
        mock_get_schemas.return_value = (mock_schemas, mock_metadata)

        client = _make_client()
        catalog = discover(client)

        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertEqual(stream_ids, ALL_STREAM_NAMES)

    @patch('tap_mailchimp.discover.get_schemas')
    def test_discover_excludes_forbidden_stream(self, mock_get_schemas):
        """Forbidden parent stream and its children are absent from the catalog."""
        mock_schemas = {name: {'properties': {}} for name in ALL_STREAM_NAMES}
        mock_metadata = {name: [] for name in ALL_STREAM_NAMES}
        mock_get_schemas.return_value = (mock_schemas, mock_metadata)

        client = _make_client(forbidden_streams=['lists'])
        catalog = discover(client)

        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn('lists', stream_ids)
        self.assertNotIn('list_members', stream_ids)
        self.assertNotIn('list_segments', stream_ids)
        self.assertNotIn('list_segment_members', stream_ids)
        self.assertIn('campaigns', stream_ids)

    @patch('tap_mailchimp.discover.get_schemas')
    def test_discover_raises_when_all_parents_forbidden(self, mock_get_schemas):
        mock_schemas = {name: {'properties': {}} for name in ALL_STREAM_NAMES}
        mock_metadata = {name: [] for name in ALL_STREAM_NAMES}
        mock_get_schemas.return_value = (mock_schemas, mock_metadata)

        client = _make_client(forbidden_streams=list(PARENT_STREAM_PATHS.keys()))
        with self.assertRaises(MailchimpForbiddenError):
            discover(client)

    @patch('tap_mailchimp.discover.get_schemas')
    def test_discover_does_not_mutate_schema_cache(self, mock_get_schemas):
        """The module-level schema cache should not be modified by access checks."""
        mock_schemas = {name: {'properties': {}} for name in ALL_STREAM_NAMES}
        mock_metadata = {name: [] for name in ALL_STREAM_NAMES}
        mock_get_schemas.return_value = (mock_schemas, mock_metadata)

        client = _make_client(forbidden_streams=['automations'])
        discover(client)

        # The original dicts returned by get_schemas should be untouched
        self.assertIn('automations', mock_schemas)


class TestDoDiscover(unittest.TestCase):
    """Tests for do_discover() delegation to discover()."""

    @patch('tap_mailchimp.__init__.discover')
    def test_do_discover_delegates_to_discover(self, mock_discover):
        """do_discover() calls discover(client) and writes the catalog to stdout."""
        client = MagicMock()
        mock_discover.return_value = MagicMock(to_dict=lambda: {})

        with patch('tap_mailchimp.__init__.json'):
            do_discover(client)

        mock_discover.assert_called_once_with(client)

    @patch('tap_mailchimp.__init__.discover')
    def test_do_discover_propagates_forbidden_error(self, mock_discover):
        """MailchimpForbiddenError from discover() (all streams forbidden) propagates."""
        from tap_mailchimp.client import MailchimpForbiddenError as FE
        client = MagicMock()
        mock_discover.side_effect = FE('all forbidden')

        with self.assertRaises(FE):
            do_discover(client)


if __name__ == '__main__':
    unittest.main()
