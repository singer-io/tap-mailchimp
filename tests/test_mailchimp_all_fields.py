from base import MailchimpBaseTest
from tap_tester import connections, menagerie, runner

# Fields to remove for which data is not generated


class MailchimpAllFields(MailchimpBaseTest):
    """Test that all fields selected for a stream are replicated."""

    def name(self):
        """Returns name of the test."""
        return "tap_tester_mailchimp_all_fields_test"

    KNOWN_MISSING_FIELDS = {
        "list_segment_members": {"interests", "last_note"},
        "campaigns": {
            "has_logo_merge_tag",
            "variate_settings",
            "rss_opts",
            "parent_campaign_id",
            "ab_split_opts",
        },
        "list_members": {"interests", "marketing_permissions"},
    }

    def test_run(self):
        """
        - Verify no unexpected streams were replicated
        - Verify that more than just the automatic fields are replicated for each stream.
        - Verify all fields for each stream are replicated
        """

        # Streams to verify all fields tests
        expected_streams = self.expected_streams() - {"automations"}
        # Need to upgrade mailchimp plan for collecting 'automations' stream data. Hence, skipping stream for now.
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        catalog_entries = [catalog for catalog in found_catalogs if catalog.get("tap_stream_id") in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries)

        stream_all_fields = dict()

        for catalog in catalog_entries:
            stream_id, stream_name = catalog["stream_id"], catalog["stream_name"]
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [
                md_entry["breadcrumb"][1] for md_entry in catalog_entry["metadata"] if md_entry["breadcrumb"] != []
            ]
            stream_all_fields[stream_name] = set(fields_from_field_level_md)

        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        self.assertSetEqual(expected_streams, synced_records.keys())

        for stream in expected_streams:
            with self.subTest(stream=stream):

                expected_all_keys = stream_all_fields[stream]
                expected_automatic_keys = self.expected_automatic_fields().get(stream)
                data = synced_records.get(stream)
                actual_all_keys = set()

                for message in data["messages"]:
                    if message["action"] == "upsert":
                        actual_all_keys.update(message["data"].keys())

                self.assertTrue(
                    expected_automatic_keys.issubset(actual_all_keys),
                    msg=f'{expected_automatic_keys-actual_all_keys} is not in "expected_all_keys"',
                )

                self.assertGreater(
                    record_count_by_stream.get(stream, -1),
                    0,
                    msg="The number of records is not over the stream max limit",
                )
                expected_all_keys = expected_all_keys - self.KNOWN_MISSING_FIELDS.get(stream, set())
                self.assertGreaterEqual(len(expected_all_keys), len(actual_all_keys))
                self.assertSetEqual(expected_all_keys, actual_all_keys)
