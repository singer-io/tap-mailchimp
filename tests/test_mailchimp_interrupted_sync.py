from tap_tester import runner, connections, menagerie
from base import MailchimpBaseTest


class MailchimpInterruptedSyncTest(MailchimpBaseTest):

    def name(self):
        return "tap_tester_mailchimp_interrupted_sync_test"

    def test_run(self):
        """
        Scenario: A sync job is interrupted. The state is saved with `currently_syncing`.
                  The next sync job kicks off and the tap picks back up on that
                  `currently_syncing` stream.
        Test Cases:
        - Verify an interrupted sync can resume based on the `currently_syncing` and
            stream level bookmark value.
        - Verify only records with replication-key values greater than or equal to the
            stream level bookmark are replicated on the resuming sync for the interrupted stream.
        - Verify the yet-to-be-synced streams are replicated following the interrupted stream
            in the resuming sync.
        """

        self.start_date = "2014-07-05T00:00:00Z"
        start_date_datetime = self.parse_date(
            self.start_date, self.START_DATE_FORMAT)

        ##########################################################################
        # First Sync
        ##########################################################################

        conn_id = connections.ensure_connection(
            self, original_properties=False)

        expected_streams = {"list_members", "reports_email_activity"}

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        catalog_entries = [
            catalog
            for catalog in found_catalogs
            if catalog.get("tap_stream_id") in expected_streams
        ]

        self.perform_and_verify_table_and_field_selection(
            conn_id, catalog_entries)

        # Run sync
        self.run_and_verify_sync(conn_id)
        synced_records_full_sync = runner.get_records_from_target_output()
        full_sync_state = menagerie.get_state(conn_id)

        ##########################################################################
        # Update the state between the syncs
        ##########################################################################

        # State to run 2nd sync
        #   reports_email_activity: remaining to sync
        #   list_members: currently getting synced
        state = {
            "currently_syncing": "lists",
            "bookmarks": {
                "lists": {
                    "8c775a04fb": {
                        "list_members": {
                            "datetime": "2022-08-11T06:24:35+00:00"
                        }
                    }
                }
            }
        }

        # Set state for 2nd sync
        menagerie.set_state(conn_id, state)

        ##########################################################################
        # Second Sync
        ##########################################################################

        # Run sync after interruption
        record_count_by_stream_interrupted_sync = self.run_and_verify_sync(
            conn_id)
        synced_records_interrupted_sync = runner.get_records_from_target_output()
        final_state = menagerie.get_state(conn_id)
        currently_syncing = final_state.get('currently_syncing')

        expected_replication_keys = self.expected_replication_keys()
        bookmark_paths = self.get_bookmark_path()

        # Checking that the resuming sync resulted in a successfully saved state
        with self.subTest():

            # Verify sync is not interrupted by checking currently_sync_stream in the state for sync
            self.assertIsNone(currently_syncing)

            # Verify bookmarks are saved
            self.assertIsNotNone(final_state.get("bookmarks"))

            # Verify final_state is equal to uninterrupted sync"s state
            # (This is what the value would have been without an
            # interruption and proves resuming succeeds)
            self.assertDictEqual(final_state, full_sync_state)

        # Stream level assertions
        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Gather Actual results
                full_records = [message['data'] for message in synced_records_full_sync.get(
                    stream, {}).get('messages', [])]
                interrupted_records = [message['data'] for message in synced_records_interrupted_sync.get(
                    stream, {}).get('messages', [])]
                interrupted_record_count = record_count_by_stream_interrupted_sync.get(
                    stream, 0)

                stream_bookmark_path = bookmark_paths.get(stream)

                final_bookmark_value = self.get_bookmark(
                    final_state.get("bookmarks"), stream_bookmark_path)
                # Gather Expectations
                expected_replication_key = list(
                    expected_replication_keys[stream])[0]

                # Verify final bookmark saved matches formatting standards for resuming sync
                self.assertIsNotNone(final_bookmark_value)
                self.assertIsInstance(final_bookmark_value, str)
                self.parse_date(str(final_bookmark_value),
                                self.BOOKMARK_DATETIME_FORMAT)

                # Currently syncing stream
                if stream == "list_members":

                    # Get parent id from bookmark
                    parent_id_value = stream_bookmark_path[1]
                    # Collect child records with parent's id (ie. list_id: "8c775a04fb") from both syncs
                    interrupted_records = [
                        rec
                        for rec in interrupted_records
                        if rec.get("list_id") == parent_id_value
                    ]
                    full_records = [
                        rec
                        for rec in full_records
                        if rec.get("list_id") == parent_id_value
                    ]

                    # Check if the interrupted stream has a bookmark written for it
                    interrupted_bookmark_datetime = self.parse_date(self.get_bookmark(
                        state.get('bookmarks'), stream_bookmark_path), self.BOOKMARK_DATETIME_FORMAT)

                    # - Verify resuming sync only replicates records with replication key values greater or equal to
                    #       the state for streams that were replicated during the interrupted sync.
                    # - Verify the interrupted sync replicates the expected record set
                    for record in interrupted_records:
                        rec_time = self.parse_date(record.get(
                            expected_replication_key), self.RECORD_DATETIME_FORMAT)
                        self.assertGreaterEqual(
                            rec_time, interrupted_bookmark_datetime)

                        self.assertIn(record, full_records,
                                      msg='Incremental table record in interrupted sync not found in full sync')

                    # Record count for all streams of interrupted sync match expectations
                    full_records_after_interrupted_bookmark = 0
                    for record in full_records:
                        rec_time = self.parse_date(record.get(
                            expected_replication_key), self.RECORD_DATETIME_FORMAT)
                        if rec_time >= interrupted_bookmark_datetime:
                            full_records_after_interrupted_bookmark += 1

                else:
                    # Sync start from the start date
                    synced_stream_datetime = start_date_datetime

                    # Verify we replicated some records for the non-interrupted streams
                    self.assertGreater(interrupted_record_count, 0)

                    # - Verify resuming sync only replicates records with replication key values greater or equal to
                    #       the state for streams that were replicated during the interrupted sync.
                    # - Verify resuming sync replicates all records that were found in the full sync (un-interupted)
                    for record in interrupted_records:
                        rec_time = self.parse_date(record.get(
                            expected_replication_key), self.RECORD_DATETIME_FORMAT)
                        self.assertGreaterEqual(
                            rec_time, synced_stream_datetime)

                        self.assertIn(record, full_records,
                                      msg='Unexpected record replicated in resuming sync.')

                    # Verify we replicated all the records from 1st sync for the streams
                    #       that are left to sync (ie. streams without bookmark in the state)
                    if stream not in state["bookmarks"].keys():
                        for record in full_records:
                            self.assertIn(record, interrupted_records,
                                          msg='Record missing from resuming sync.')
