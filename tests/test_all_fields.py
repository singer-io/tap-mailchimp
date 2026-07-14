"""Test that with all fields selected, every schema field is replicated."""
from base import MailchimpBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


# Fields present in the API response but intentionally omitted from the schema.
# List any here to prevent the AllFieldsTest from failing on them.
KNOWN_MISSING_FIELDS = {
    # "stream_name": {"field_name"},
}


class MailchimpAllFieldsTest(AllFieldsTest, MailchimpBaseTest):
    """
    With all fields selected, every field declared in the schema must appear
    in at least one emitted record.
    """

    @staticmethod
    def name():
        return "tap_tester_mailchimp_all_fields_test"

    # Passed to AllFieldsTest so known gaps are skipped gracefully.
    fields_to_remove = KNOWN_MISSING_FIELDS

    def streams_to_test(self):
        return self.expected_stream_names()
