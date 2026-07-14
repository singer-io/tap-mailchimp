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
        return self.expected_stream_names()

    def manipulate_state(self):
        """
        Return a synthetic interrupted-sync state.

        Simulates a sync that was interrupted while processing campaigns,
        with bookmarks already written for the two incremental streams.

        list_members bookmarks are nested per list_id under the `lists` key.
        reports_email_activity bookmarks are per campaign_id under their own key.
        """
        return {
            # Mailchimp uses `current_stream` for resumption tracking.
            "current_stream": "campaigns",
            "bookmarks": {
                # list_members: bookmarked per list_id
                "lists": {
                    "REPLACE_WITH_REAL_LIST_ID": {
                        "list_members": {
                            "datetime": "2020-01-01T00:00:00Z"
                        }
                    }
                },
                # reports_email_activity: bookmarked per campaign_id
                "reports_email_activity": {
                    "REPLACE_WITH_REAL_CAMPAIGN_ID": "2020-01-01T00:00:00Z"
                },
            },
        }
