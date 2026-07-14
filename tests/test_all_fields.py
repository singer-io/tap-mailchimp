"""Test that with all fields selected, every schema field is replicated."""
from base import MailchimpBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest



class MailchimpAllFieldsTest(AllFieldsTest, MailchimpBaseTest):
    """
    With all fields selected, every field declared in the schema must appear
    in at least one emitted record.
    """

    MISSING_FIELDS = {
        "campaigns": {"has_logo_merge_tag"},
        "list_segment_members": {"interests"},
        "reports_email_activity": {"url", "type"}
    }

    @staticmethod
    def name():
        return "tap_tester_mailchimp_all_fields_test"

    def streams_to_test(self):
        streams_to_exclude = {
            'automations',  # no data in test account
        }
        return self.expected_stream_names().difference(streams_to_exclude)
