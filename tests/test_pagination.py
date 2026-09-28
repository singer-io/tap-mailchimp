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


class MailchimpPaginationTest(PaginationTest, MailchimpBaseTest):
    """
    Ensure tap can replicate multiple pages of data for high-volume streams.

    The test account has small data volumes, so we set page_size=1 so that any
    stream with ≥2 records exercises multi-page behaviour.
    """

    page_size = 1

    @staticmethod
    def name():
        return "tap_tester_mailchimp_pagination_test"

    def streams_to_test(self):
        streams_to_exclude = {
            "automations",       # typically very few automation workflows
        }
        return self.expected_stream_names().difference(streams_to_exclude)
