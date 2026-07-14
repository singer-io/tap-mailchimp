"""Test that with no fields selected, only automatic fields are still replicated."""
from base import MailchimpBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class MailchimpAutomaticFieldsTest(MinimumSelectionTest, MailchimpBaseTest):
    """
    With no extra fields selected, only automatic fields (primary keys +
    replication keys) must appear in every emitted record.
    """

    @staticmethod
    def name():
        return "tap_tester_mailchimp_automatic_fields_test"

    def streams_to_test(self):
        streams_to_exclude = {
            'automations',  # no data in test account
        }
        return self.expected_stream_names().difference(streams_to_exclude)
