"""Base test class for tap-mailchimp tap-tester integration tests."""
import os

from tap_tester import connections, menagerie, runner
from tap_tester.base_suite_tests.base_case import BaseCase


class MailchimpBaseTest(BaseCase):
    """
    Base class shared by all tap-mailchimp integration tests.

    Defines stream metadata (primary keys, replication method/keys,
    parent-stream relationships) used by the standard tap-tester
    base_suite_tests (DiscoveryTest, BookmarkTest, MinimumSelectionTest,
    AllFieldsTest, etc.).
    """
    start_date = "2019-09-01T00:00:00Z"

    # ------------------------------------------------------------------ #
    # tap-tester identity
    # ------------------------------------------------------------------ #

    @staticmethod
    def tap_name():
        return "tap-mailchimp"

    @staticmethod
    def get_type():
        return "platform.mailchimp"

    @staticmethod
    def get_credentials():
        return {
            "client_id":     os.getenv("TAP_MAILCHIMP_CLIENT_ID"),
            "client_secret": os.getenv("TAP_MAILCHIMP_CLIENT_SECRET"),
            "access_token":  os.getenv("TAP_MAILCHIMP_ACCESS_TOKEN"),
        }

    # Subclasses may set page_size to override the tap default (1000).
    page_size = None

    def get_properties(self):
        """Configuration of properties required for the tap."""
        return_value = {"start_date": self.start_date}
        if self.page_size is not None:
            return_value["page_size"] = str(self.page_size)
        return return_value

    def expected_page_size(self, stream=None):
        size = self.page_size if self.page_size is not None else 1000
        page_sizes = {s: size for s in self.expected_stream_names()}
        if stream is None:
            return page_sizes
        return page_sizes[stream]

    # ------------------------------------------------------------------ #
    # Stream metadata
    # ------------------------------------------------------------------ #

    @classmethod
    def expected_metadata(cls):
        """
        Full catalog of expected stream metadata.

        Keys used by base_suite_tests:
          PRIMARY_KEYS            - set of key property field names
          REPLICATION_METHOD      - cls.INCREMENTAL or cls.FULL_TABLE
          REPLICATION_KEYS        - set of replication key field names (empty for FULL_TABLE)
          OBEYS_START_DATE        - whether the stream filters by start_date
          API_LIMIT               - page size used by the tap (default 1000)

        Optional keys:
          PARENT_TAP_STREAM_ID    - parent stream name (child streams only)
        """
        return {
            "automations": {
                cls.PRIMARY_KEYS:         {"id"},
                cls.REPLICATION_METHOD:   cls.FULL_TABLE,
                cls.REPLICATION_KEYS:     set(),
                cls.OBEYS_START_DATE:     False,
                cls.API_LIMIT:            100,
            },
            "campaigns": {
                cls.PRIMARY_KEYS:         {"id"},
                cls.REPLICATION_METHOD:   cls.FULL_TABLE,
                cls.REPLICATION_KEYS:     set(),
                cls.OBEYS_START_DATE:     False,
                cls.API_LIMIT:            100,
            },
            "lists": {
                cls.PRIMARY_KEYS:         {"id"},
                cls.REPLICATION_METHOD:   cls.FULL_TABLE,
                cls.REPLICATION_KEYS:     set(),
                cls.OBEYS_START_DATE:     False,
                cls.API_LIMIT:            100,
            },
            "list_members": {
                cls.PRIMARY_KEYS:         {"id", "list_id"},
                cls.REPLICATION_METHOD:   cls.INCREMENTAL,
                cls.REPLICATION_KEYS:     {"last_changed"},
                cls.OBEYS_START_DATE:     True,
                cls.API_LIMIT:            100,
                "parent-tap-stream-id":   "lists",
            },
            "list_segments": {
                cls.PRIMARY_KEYS:         {"id", "list_id"},
                cls.REPLICATION_METHOD:   cls.FULL_TABLE,
                cls.REPLICATION_KEYS:     set(),
                cls.OBEYS_START_DATE:     False,
                cls.API_LIMIT:            100,
                "parent-tap-stream-id":   "lists",
            },
            "list_segment_members": {
                cls.PRIMARY_KEYS:         {"id", "list_id", "segment_id"},
                cls.REPLICATION_METHOD:   cls.FULL_TABLE,
                cls.REPLICATION_KEYS:     set(),
                cls.OBEYS_START_DATE:     False,
                cls.API_LIMIT:            100,
                "parent-tap-stream-id":   "list_segments",
            },
            "reports_email_activity": {
                cls.PRIMARY_KEYS:         {"campaign_id", "action", "email_id", "timestamp"},
                cls.REPLICATION_METHOD:   cls.INCREMENTAL,
                cls.REPLICATION_KEYS:     {"timestamp"},
                cls.OBEYS_START_DATE:     True,
                cls.API_LIMIT:            100,
                "parent-tap-stream-id":   "campaigns",
            },
            "unsubscribes": {
                cls.PRIMARY_KEYS:         {"campaign_id", "email_id"},
                cls.REPLICATION_METHOD:   cls.FULL_TABLE,
                cls.REPLICATION_KEYS:     set(),
                cls.OBEYS_START_DATE:     False,
                cls.API_LIMIT:            100,
                "parent-tap-stream-id":   "campaigns",
            },
        }


    def expected_stream_names(self):
        return set(self.expected_metadata().keys())

    def incremental_streams(self):
        return {
            s for s, m in self.expected_metadata().items()
            if m[self.REPLICATION_METHOD] == self.INCREMENTAL
        }
