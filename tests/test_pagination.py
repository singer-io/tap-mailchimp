"""
Test that the tap can replicate multiple pages of data for streams that
support pagination.

Mailchimp uses offset-based pagination via the `count` (page size) and
`offset` query parameters. The tap's page size defaults to 1000 and is
configurable via the `page_size` config key.

PaginationTest verifies that the tap emits more records than a single page
can hold for at least one stream, confirming pagination is working end-to-end.
"""
from base import MailchimpBaseTest
from tap_tester.base_suite_tests.pagination_test import PaginationTest


# Streams that are unlikely to have enough data to trigger a second page in
# a typical test account. Exclude them to avoid false negatives.
_STREAMS_WITHOUT_ENOUGH_DATA = {
    "automations",          # typically very few automation workflows
    "list_segment_members", # depends on parent segments having 1000+ members
}


class MailchimpPaginationTest(PaginationTest, MailchimpBaseTest):
    """
    Ensure tap can replicate multiple pages of data for high-volume streams.
    """

    @staticmethod
    def name():
        return "tap_tester_mailchimp_pagination_test"

    def streams_to_test(self):
        return self.expected_stream_names().difference(_STREAMS_WITHOUT_ENOUGH_DATA)
