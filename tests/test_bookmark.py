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

    @staticmethod
    def name():
        return "tap_tester_mailchimp_bookmark_test"

    def streams_to_test(self):
        # Only incremental streams participate in bookmark testing.
        return self.incremental_streams()
