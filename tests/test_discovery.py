"""Test tap discovery mode and catalog metadata."""
from base import MailchimpBaseTest
from tap_tester import menagerie
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest


# Streams that have no parent (i.e. they are root streams, not children).
_ORPHAN_STREAMS = {"automations", "campaigns", "lists"}


class MailchimpDiscoveryTest(DiscoveryTest, MailchimpBaseTest):
    """
    Verify discovery returns the correct streams, primary keys, replication
    methods, replication keys, and parent-tap-stream-id metadata.

    Inherits the standard assertion battery from DiscoveryTest and supplies
    tap-specific metadata via MailchimpBaseTest.expected_metadata().
    """

    @staticmethod
    def name():
        return "tap_tester_mailchimp_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()

    def test_parent_stream_metadata(self):
        """
        Verify that every child stream's catalog entry carries the correct
        parent-tap-stream-id in its root metadata breadcrumb.
        """
        conn_id = self.conn_id  # set by DiscoveryTest.setUp()
        for stream, meta in self.expected_metadata().items():
            expected_parent = meta.get("parent-tap-stream-id")
            catalog = next(
                c for c in self.found_catalogs
                if c["stream_name"] == stream
            )
            root_metadata = next(
                m["metadata"]
                for m in menagerie.get_annotated_schema(
                    conn_id, catalog["stream_id"]
                )["metadata"]
                if m["breadcrumb"] == []
            )

            if stream in _ORPHAN_STREAMS:
                self.assertNotIn(
                    "parent-tap-stream-id", root_metadata,
                    msg=f"{stream} should not have a parent-tap-stream-id",
                )
            else:
                self.assertEqual(
                    root_metadata.get("parent-tap-stream-id"),
                    expected_parent,
                    msg=(
                        f"{stream}: expected parent-tap-stream-id="
                        f"{expected_parent!r}, got "
                        f"{root_metadata.get('parent-tap-stream-id')!r}"
                    ),
                )
