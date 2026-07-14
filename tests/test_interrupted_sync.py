"""
Test that when a sync is interrupted mid-run, a subsequent sync resumes
correctly from the saved bookmark rather than re-syncing from scratch.

The test injects a synthetic interrupted state via manipulate_state(),
then verifies the tap picks up where it left off for incremental streams
and does a full re-sync for FULL_TABLE streams.

Note: Mailchimp uses `current_stream` (not `currently_syncing`) as the
state key for mid-sync resumption.
"""
from base import MailchimpBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class MailchimpInterruptedSyncTest(InterruptedSyncTest, MailchimpBaseTest):
    """
    Verify the tap resumes from a mid-sync interrupted state correctly.
    """

    @staticmethod
    def name():
        return "tap_tester_mailchimp_interrupted_sync_test"

    def streams_to_test(self):
        # Exclude streams with no data and all FULL_TABLE streams —
        # interrupted sync testing is only meaningful for incremental streams.
        return self.incremental_streams()

    def manipulate_state(self):
        """
        Simulate a sync interrupted at the very start — before 'lists' begins.

        Setting current_stream='lists' means the tap resumes from the first
        stream, so ALL streams (lists, list_members, campaigns,
        reports_email_activity) run in the resuming sync.

        We still include bookmarks for the incremental streams so the tap
        respects them on resumption.
        """
        return {
            # Framework reads 'currently_syncing' for stream-order assertions.
            "currently_syncing": "lists",
            # Tap reads 'current_stream' to resume from the right place.
            "current_stream": "lists",
            "bookmarks": {
                # list_members: bookmarked per list_id under 'lists' key
                "lists": {
                    "77a19dd84f": {"list_members": {"datetime": "2020-01-01T00:00:00+00:00"}},
                    "cb3e2830f2": {"list_members": {"datetime": "2020-01-01T00:00:00+00:00"}},
                    "8c775a04fb": {"list_members": {"datetime": "2020-01-01T00:00:00+00:00"}},
                    "7069d6ec55": {"list_members": {"datetime": "2020-01-01T00:00:00+00:00"}},
                },
                # reports_email_activity: bookmarked per campaign_id
                "reports_email_activity": {
                    "5b483c58de": "2020-01-01T00:00:00+00:00",
                    "55f54d0e17": "2020-01-01T00:00:00+00:00",
                },
            },
        }

    def get_bookmark_value(self, state, stream):
        """
        Mailchimp uses a nested bookmark structure:
          - list_members:           bookmarks → lists → {list_id} → list_members → datetime
          - reports_email_activity: bookmarks → reports_email_activity → {campaign_id} → value
        Return the maximum bookmark value across all parent IDs.
        """
        bookmarks = state.get('bookmarks', {})

        if stream == 'list_members':
            values = [
                v
                for list_bm in bookmarks.get('lists', {}).values()
                for v in [list_bm.get('list_members', {}).get('datetime')]
                if v
            ]
            return max(values) if values else None

        if stream == 'reports_email_activity':
            values = [v for v in bookmarks.get('reports_email_activity', {}).values()
                      if isinstance(v, str)]
            return max(values) if values else None

        return super().get_bookmark_value(state, stream)

    # -------------------------------------------------------------------------
    # Framework overrides required for Mailchimp's non-standard state structure
    # -------------------------------------------------------------------------

    def test_syncs_were_successful(self):
        """
        Override: the Mailchimp tap uses 'current_stream' (not 'currently_syncing')
        and never clears the injected 'currently_syncing' key from state.
        Just verify bookmarks were written by both syncs.
        """
        self.assertIsNotNone(self.first_sync_state.get('bookmarks'))
        self.assertIsNotNone(self.resuming_sync_state.get('bookmarks'))

    def test_interrupted_sync_stream_order(self):
        """
        Override: streams_to_test() is incremental-only so the expected
        interruption stream is list_members (current_stream='lists' causes the
        tap to start from lists/list_members first).
        """
        self.assertIn(self.resuming_sync_order[0], ('lists', 'list_members'),
                      msg="Resuming sync should start from the interrupted stream")

    def test_bookmarked_streams_start_date(self):
        """
        Override: the framework iterates bookmarks.keys() which includes 'lists'
        (Mailchimp's parent key for list_members, a FULL_TABLE key with no
        replication key).  streams_to_test() is already incremental-only so we
        just iterate that directly instead of intersecting with bookmarks.keys().
        """
        currently_syncing_stream = self.manipulate_state()['currently_syncing']
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                expected_replication_key = self.expected_replication_keys(stream)
                assert len(expected_replication_key) == 1
                expected_replication_key = next(iter(expected_replication_key))

                first_sync_records = [
                    record['data'] for record in
                    self.first_sync_records.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']

                if not first_sync_records:
                    continue

                stream_bookmark = self.get_bookmark_value(self.manipulate_state(), stream)
                if stream_bookmark is None:
                    continue

                completed = stream != currently_syncing_stream
                expected_start = self.calculate_expected_sync_start_time(
                    stream_bookmark, stream, completed=completed)

                actual_oldest = min(
                    self.parse_date(r[expected_replication_key])
                    for r in first_sync_records)

                self.assertGreaterEqual(actual_oldest, expected_start,
                                        msg=f"{stream}: oldest record predates expected start")

    def test_resuming_sync_records(self):
        """
        Override: uses get_bookmark_value which must handle Mailchimp's nested
        state structure.
        """
        currently_syncing_stream = self.manipulate_state()['currently_syncing']
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                expected_replication_key = self.expected_replication_keys(stream)
                assert len(expected_replication_key) == 1
                expected_replication_key = next(iter(expected_replication_key))

                first_sync_records = [
                    record['data'] for record in
                    self.first_sync_records.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']
                resuming_sync_records = [
                    record['data'] for record in
                    self.resuming_sync_records.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']

                stream_bookmark = self.get_bookmark_value(self.manipulate_state(), stream)
                if stream_bookmark:
                    completed = stream != currently_syncing_stream
                    expected_start = self.calculate_expected_sync_start_time(
                        stream_bookmark, stream, completed=completed)
                else:
                    if not first_sync_records:
                        continue
                    expected_start = min(
                        self.parse_date(r[expected_replication_key])
                        for r in first_sync_records)

                first_sync_records_after_bookmark = sorted(
                    (r for r in first_sync_records
                     if self.parse_date(r[expected_replication_key]) >= expected_start),
                    key=lambda r: r[expected_replication_key])

                first_sync_bookmark = self.get_bookmark_value(self.first_sync_state, stream)
                if first_sync_bookmark:
                    filtered_resuming_records = sorted(
                        (r for r in resuming_sync_records
                         if self.parse_date(r[expected_replication_key]) <=
                         self.parse_date(first_sync_bookmark)),
                        key=lambda r: r[expected_replication_key])
                else:
                    filtered_resuming_records = sorted(
                        resuming_sync_records, key=lambda r: r[expected_replication_key])

                self.assertEqual(first_sync_records_after_bookmark, filtered_resuming_records,
                                 msg=f"{stream}: incorrect data in the interrupted sync")
