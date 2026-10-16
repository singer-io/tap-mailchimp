"""
Test that the tap respects the configured start_date.

Runs two syncs with different start dates and verifies that:
  - Sync with a more recent start_date returns fewer (or equal) records
    than a sync with an older start_date for streams that obey start_date.
  - FULL_TABLE streams are unaffected by start_date changes.

Only streams with OBEYS_START_DATE=True in expected_metadata() are tested.
"""
from base import MailchimpBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class MailchimpStartDateTest(StartDateTest, MailchimpBaseTest):
    """
    Verify the tap returns fewer records when start_date is set to a more
    recent date for streams that filter by start_date.
    """

    @staticmethod
    def name():
        return "tap_tester_mailchimp_start_date_test"

    def streams_to_test(self):
        # Only streams that actually filter on start_date are meaningful here.
        return {
            stream
            for stream, meta in self.expected_metadata().items()
            if meta[self.OBEYS_START_DATE]
        }

    @property
    def start_date_1(self):
        """Older start date — should return more records."""
        return "2015-01-01T00:00:00Z"

    @property
    def start_date_2(self):
        """More recent start date — should return fewer records.
        """
        return "2023-01-01T00:00:00Z"
