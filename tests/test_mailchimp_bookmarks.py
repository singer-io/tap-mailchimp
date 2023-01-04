import tap_tester.connections as connections
import tap_tester.runner as runner
from base import MailchimpBaseTest
from tap_tester import menagerie


class MailchimpBookMark(MailchimpBaseTest):
    """Test that tap sets a bookmark and respects it for the next sync of a
    stream."""

    def name(self):
        return "tap_tester_mailchimp_bookmark_test"

    def test_run(self):
        """
        - Verify for each incremental stream you can do a sync which records bookmarks, and that the format matches expectations.
        - Verify that a bookmark doesn't exist for full table streams.
        - Verify the bookmark is the max value sent to the target for the a given replication key.
        - Verify 2nd sync respects the bookmark. All data of the 2nd sync is >= the bookmark from the first sync The number of records in the 2nd sync is less then the first
        """

        # Need to upgrade mailchimp plan for collecting 'automations' stream data. Hence, skipping the stream for now.
        expected_streams = self.expected_streams() - {"automations"}

        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

        ##########################################################################
        # First Sync
        ##########################################################################
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        catalog_entries = [catalog for catalog in found_catalogs if catalog.get("tap_stream_id") in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries)

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Update State Between Syncs
        ##########################################################################

        new_states = {
            "bookmarks": {
                "lists": {
                    "8c775a04fb": {  # Verifying bookmark for single list's 'list_members'
                        "list_members": {"datetime": "2022-08-01T06:23:35.000000Z"}
                    }
                },
                "reports_email_activity_next_chunk": 0,
                "reports_email_activity_last_run_id": None,
                "reports_email_activity": {  # Verifying bookmark for single campaign's 'reports_email_activity'
                    "32e6edcecb": "2016-05-15T18:57:16.000000Z"
                },
                "unsubscribes": {  # Verifying bookmark for single campaign's 'unsubscribers'
                    "5b483c58de": {"timestamp": "2014-10-23T23:37:21.000000Z"}
                },
            }
        }

        menagerie.set_state(conn_id, new_states)

        ##########################################################################
        # Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Test By Stream
        ##########################################################################

        bookmark_path = self.get_bookmark_path()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_replication_method = expected_replication_methods[stream]
                stream_bookmark_path = bookmark_path.get(stream)

                # Collect information for assertions from syncs 1 & 2 based on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [
                    record.get("data")
                    for record in first_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]
                second_sync_messages = [
                    record.get("data")
                    for record in second_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]
                first_bookmark_value = self.get_bookmark(first_sync_bookmarks.get("bookmarks"), stream_bookmark_path)
                second_bookmark_value = self.get_bookmark(second_sync_bookmarks.get("bookmarks"), stream_bookmark_path)

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(
                    second_sync_count,
                    0,
                    msg=f"We are not fully testing bookmarking for {stream}",
                )
                
                if expected_replication_method == self.INCREMENTAL:
                    # Get parent key in child's record
                    parent_id = "campaign_id" if stream == "reports_email_activity" else "list_id"
                    # Get parent id from bookmark
                    parent_id_value = stream_bookmark_path[1]
                    # Collect child records with parent's id from both syncs
                    first_sync_messages = [
                        record for record in first_sync_messages if record.get(parent_id) == parent_id_value
                    ]
                    second_sync_messages = [
                        record for record in second_sync_messages if record.get(parent_id) == parent_id_value
                    ]

                    replication_key = list(expected_replication_keys[stream])[0]

                    first_bookmark_value_ts = self.parse_date(first_bookmark_value, self.BOOKMARK_DATETIME_FORMAT)

                    second_bookmark_value_ts = self.parse_date(second_bookmark_value, self.BOOKMARK_DATETIME_FORMAT)

                    simulated_bookmark_value = self.parse_date(
                        self.get_bookmark(new_states.get("bookmarks"), stream_bookmark_path),
                        self.BOOKMARK_DATETIME_FORMAT,
                    )

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_value)

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_value)

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    self.assertEqual(second_bookmark_value, first_bookmark_value)

                    for record in first_sync_messages:

                        # Verify the first sync bookmark value is the max replication key value for a given stream
                        replication_key_value = self.parse_date(
                            record.get(replication_key), self.BOOKMARK_DATETIME_FORMAT
                        )

                        self.assertLessEqual(
                            replication_key_value,
                            first_bookmark_value_ts,
                            msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced.",
                        )

                    for record in second_sync_messages:
                        # Verify the second sync replication key value is Greater or Equal to the first sync bookmark
                        replication_key_value = self.parse_date(
                            record.get(replication_key), self.BOOKMARK_DATETIME_FORMAT
                        )
                        self.assertGreaterEqual(
                            replication_key_value,
                            simulated_bookmark_value,
                            msg="Second sync records do not respect the previous bookmark.",
                        )

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(
                            replication_key_value,
                            second_bookmark_value_ts,
                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.",
                        )
                
                        # Verify that you get less data the 2nd time around
                        self.assertLess(
                            second_sync_count,
                            first_sync_count,
                            msg="second sync didn't have less records, bookmark usage not verified",
                        )

                        # Verify at least 1 record was replicated in the second sync
                        self.assertGreater(
                            second_sync_count,
                            0,
                            msg=f"We are not fully testing bookmarking for {stream}",
                        )


                elif expected_replication_method == self.FULL_TABLE:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_value)
                    self.assertIsNone(second_bookmark_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)

                    # Verify that for the full table stream, all data replicated in sync 1 is replicated again in sync 2
                    for record in second_sync_messages:
                        self.assertIn(record, first_sync_messages)
                else:

                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(
                            stream, expected_replication_method
                        )
                    )

