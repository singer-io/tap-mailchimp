import json

from base import MailchimpBaseTest
from tap_tester import connections, runner


class MailchimpAutomaticFields(MailchimpBaseTest):
    """Ensure running the tap with all streams selected and all fields
    deselected results in the replication of just the primary keys and
    replication keys (automatic fields)."""

    def name(self):
        return "tap_tester_mailchimp_automatic_fields_test"

    def test_run(self):
        """
        - Verify that when no fields are selected and only the automatic fields are replicated.
        - Verify that all replicated records have unique primary key values.
        - Verify that you get records for all streams
        """

        expected_streams = self.expected_streams()

        # Need to upgrade mailchimp plan for collecting 'automations' stream data. Hence, skipping stream for now.
        expected_streams = expected_streams - {"automations"}

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_automatic_fields = [
            catalog for catalog in found_catalogs if catalog.get("tap_stream_id") in expected_streams
        ]

        # Select all streams and no fields within streams
        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_automatic_fields, select_all_fields=False
        )

        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_keys = self.expected_automatic_fields().get(stream)
                expected_primary_keys = self.expected_primary_keys()[stream]

                # Collect actual values
                data = synced_records.get(stream, {})
                record_messages_keys = [set(row["data"].keys()) for row in data.get("messages", [])]
                records = [
                    message.get("data") for message in data.get("messages", []) if message.get("action") == "upsert"
                ]

                primary_keys_list = [
                    tuple(message.get(expected_pk) for expected_pk in expected_primary_keys)
                    for message in [json.loads(t) for t in {json.dumps(d) for d in records}]
                ]
                unique_primary_keys_list = set(primary_keys_list)

                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1),
                    0,
                    msg="The number of records is not over the stream min limit",
                )

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    # Not all records include 'ip'.
                    # But for our dataset 'ip' as differentiating field among non-similar records
                    if stream == "reports_email_activity":
                        expected_keys -= {"ip"}
                        actual_keys -= {"ip"}
                    self.assertSetEqual(expected_keys, actual_keys)

                # Verify that all replicated records have unique primary key values.
                self.assertEqual(
                    len(primary_keys_list),
                    len(unique_primary_keys_list),
                    msg="Replicated record does not have unique primary key values.",
                )
