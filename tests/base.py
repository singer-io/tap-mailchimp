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

    start_date = "2019-01-01T00:00:00Z"

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

    def get_properties(self, original: bool = True):
        return_value = {"start_date": "2019-09-01T00:00:00Z"}
        if original:
            return return_value
        return_value["start_date"] = self.start_date
        return return_value

    # ------------------------------------------------------------------ #
    # Stream metadata
    # ------------------------------------------------------------------ #

    @classmethod
    def expected_metadata(cls):
        """
        Full catalog of expected stream metadata.

        Keys used by base_suite_tests:
          PRIMARY_KEYS            – set of key property field names
          REPLICATION_METHOD      – cls.INCREMENTAL or cls.FULL_TABLE
          REPLICATION_KEYS        – set of replication key field names (empty for FULL_TABLE)
          OBEYS_START_DATE        – whether the stream filters by start_date
          API_LIMIT               – page size used by the tap (default 1000)

        Optional keys:
          PARENT_TAP_STREAM_ID    – parent stream name (child streams only)
        """
        return {
            # ---- top-level streams ------------------------------------ #
            "automations": {
                cls.PRIMARY_KEYS:         {"id"},
                cls.REPLICATION_METHOD:   cls.FULL_TABLE,
                cls.REPLICATION_KEYS:     set(),
                cls.OBEYS_START_DATE:     False,
                cls.API_LIMIT:            1000,
            },
            "campaigns": {
                cls.PRIMARY_KEYS:         {"id"},
                cls.REPLICATION_METHOD:   cls.FULL_TABLE,
                cls.REPLICATION_KEYS:     set(),
                cls.OBEYS_START_DATE:     False,
                cls.API_LIMIT:            1000,
            },
            "lists": {
                cls.PRIMARY_KEYS:         {"id"},
                cls.REPLICATION_METHOD:   cls.FULL_TABLE,
                cls.REPLICATION_KEYS:     set(),
                cls.OBEYS_START_DATE:     False,
                cls.API_LIMIT:            1000,
            },
            # ---- children of lists ------------------------------------ #
            "list_members": {
                cls.PRIMARY_KEYS:         {"id", "list_id"},
                cls.REPLICATION_METHOD:   cls.INCREMENTAL,
                cls.REPLICATION_KEYS:     {"last_changed"},
                cls.OBEYS_START_DATE:     True,
                cls.API_LIMIT:            1000,
                "parent-tap-stream-id":   "lists",
            },
            "list_segments": {
                cls.PRIMARY_KEYS:         {"id"},
                cls.REPLICATION_METHOD:   cls.FULL_TABLE,
                cls.REPLICATION_KEYS:     set(),
                cls.OBEYS_START_DATE:     False,
                cls.API_LIMIT:            1000,
                "parent-tap-stream-id":   "lists",
            },
            "list_segment_members": {
                cls.PRIMARY_KEYS:         {"id"},
                cls.REPLICATION_METHOD:   cls.FULL_TABLE,
                cls.REPLICATION_KEYS:     set(),
                cls.OBEYS_START_DATE:     False,
                cls.API_LIMIT:            1000,
                "parent-tap-stream-id":   "list_segments",
            },
            # ---- children of campaigns -------------------------------- #
            "reports_email_activity": {
                cls.PRIMARY_KEYS:         {"campaign_id", "action", "email_id", "timestamp"},
                cls.REPLICATION_METHOD:   cls.INCREMENTAL,
                cls.REPLICATION_KEYS:     {"timestamp"},
                cls.OBEYS_START_DATE:     True,
                cls.API_LIMIT:            1000,
                "parent-tap-stream-id":   "campaigns",
            },
            "unsubscribes": {
                cls.PRIMARY_KEYS:         {"campaign_id", "email_id"},
                cls.REPLICATION_METHOD:   cls.FULL_TABLE,
                cls.REPLICATION_KEYS:     set(),
                cls.OBEYS_START_DATE:     False,
                cls.API_LIMIT:            1000,
                "parent-tap-stream-id":   "campaigns",
            },
        }

    # ------------------------------------------------------------------ #
    # Convenience helpers used by multiple test classes
    # ------------------------------------------------------------------ #

    def expected_stream_names(self):
        return set(self.expected_metadata().keys())

    def expected_primary_keys(self):
        return {
            stream: meta[self.PRIMARY_KEYS]
            for stream, meta in self.expected_metadata().items()
        }

    def expected_replication_method(self):
        return {
            stream: meta[self.REPLICATION_METHOD]
            for stream, meta in self.expected_metadata().items()
        }

    def expected_replication_keys(self):
        return {
            stream: meta[self.REPLICATION_KEYS]
            for stream, meta in self.expected_metadata().items()
        }

    def expected_automatic_fields(self):
        """
        Return a dict of stream → set of automatic fields.
        Automatic fields = primary keys ∪ replication keys.
        """
        auto = {}
        for stream, meta in self.expected_metadata().items():
            auto[stream] = meta[self.PRIMARY_KEYS] | meta[self.REPLICATION_KEYS]
        return auto

    def incremental_streams(self):
        return {
            s for s, m in self.expected_metadata().items()
            if m[self.REPLICATION_METHOD] == self.INCREMENTAL
        }

    def full_table_streams(self):
        return {
            s for s, m in self.expected_metadata().items()
            if m[self.REPLICATION_METHOD] == self.FULL_TABLE
        }
