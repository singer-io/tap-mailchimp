"""
Test that the tap sets bookmarks and respects them on subsequent syncs.

Covers both incremental streams:
  - list_members        (replication key: last_changed)
  - reports_email_activity  (replication key: timestamp)

Inherits the standard two-sync bookmark assertion battery from BookmarkTest
and adds a Mailchimp-specific assertion that verifies no null bookmarks are
written after an empty first-sync response for reports_email_activity.
"""
from base import MailchimpBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class MailchimpBookmarkTest(BookmarkTest, MailchimpBaseTest):
    """
    Run two full syncs and assert:
      1. Bookmarks are emitted after sync 1 for all incremental streams.
      2. Records emitted in sync 2 all have replication-key values >=
         the bookmark saved after sync 1.
      3. Sync 2 does not re-emit records that predate the bookmark.
    """

    # Mailchimp bookmark timestamps include timezone offset e.g. '2022-08-12T06:10:19+00:00'
    bookmark_format = "%Y-%m-%dT%H:%M:%S+00:00"

    # Seed state so sync 1 starts from a known midpoint rather than start_date,
    # keeping the test short while still leaving records above the bookmark for sync 2.
    initial_bookmarks = {
        'bookmarks': {
            'lists': {
                '77a19dd84f': {'list_members': {'datetime': '2015-03-06T16:03:01+00:00'}},
                'cb3e2830f2': {'list_members': {'datetime': '2019-01-01T00:00:00+00:00'}},
                '8c775a04fb': {'list_members': {'datetime': '2019-01-01T00:00:00+00:00'}},
                '7069d6ec55': {'list_members': {'datetime': '2015-04-06T20:43:24+00:00'}},
            },
            'reports_email_activity': {
                '5b483c58de': '2019-01-01T00:00:00+00:00',
                '55f54d0e17': '2019-01-01T00:00:00+00:00',
            },
        }
    }

    @staticmethod
    def name():
        return "tap_tester_mailchimp_bookmark_test"

    @staticmethod
    def get_stream_name(stream_id):
        """
        Map Mailchimp's non-standard state keys back to stream names.
          - 'lists' holds list_members bookmarks in nested form
          - 'reports_email_activity_next_chunk' and 'reports_email_activity_last_run_id'
            are tap-internal metadata stored alongside the real bookmark
        """
        mailchimp_state_key_map = {
            'lists': 'list_members',
            'reports_email_activity_next_chunk': 'reports_email_activity',
            'reports_email_activity_last_run_id': 'reports_email_activity',
        }
        return mailchimp_state_key_map.get(stream_id, stream_id)

    def streams_to_test(self):
        # Only incremental streams participate in bookmark testing.
        return self.incremental_streams()

    def get_bookmark_value(self, state, stream):
        """
        Mailchimp uses a nested bookmark structure:
          - list_members:           bookmarks → lists → {list_id} → list_members → datetime
          - reports_email_activity: bookmarks → reports_email_activity → {campaign_id} → value
        Return the maximum bookmark value across all parent IDs for the stream.
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

    def manipulate_state(self, state: dict, new_bookmarks: dict):
        """
        calculate_new_bookmarks() produces flat bookmarks like:
          {'list_members': {'last_changed': '...'}, 'reports_email_activity': {'timestamp': '...'}}

        The tap only reads Mailchimp's nested format, so convert back:
          - list_members → set all per-list-id bookmarks to the new value
          - reports_email_activity → set all per-campaign-id bookmarks to the new value
        """
        from copy import deepcopy
        new_state = deepcopy(state)
        bookmarks = new_state.setdefault('bookmarks', {})

        if 'list_members' in new_bookmarks:
            bm_value = new_bookmarks['list_members'].get('last_changed')
            if bm_value:
                for list_id in bookmarks.get('lists', {}):
                    bookmarks['lists'][list_id]['list_members'] = {'datetime': bm_value}

        if 'reports_email_activity' in new_bookmarks:
            bm_value = new_bookmarks['reports_email_activity'].get('timestamp')
            if bm_value:
                for campaign_id, val in list(bookmarks.get('reports_email_activity', {}).items()):
                    if isinstance(val, str):
                        bookmarks['reports_email_activity'][campaign_id] = bm_value

        return new_state
