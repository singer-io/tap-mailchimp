from math import ceil

from base import MailchimpBaseTest
from tap_tester import connections, runner


class MailchimpPagination(MailchimpBaseTest):
    """Ensure tap can replicate multiple pages of data for streams that use
    pagination."""

    page_size = 2

    def name(self):
        """Returns name of the test."""
        return "tap_tester_mailchimp_pagination_test"

    def get_properties(self, original=True):
        """Configuration properties required for the tap."""
        props = super().get_properties(original)
        props["page_size"] = self.page_size
        return props

    def test_run(self):
        """
        - Verify that for each stream you can get multiple pages of data.
          This requires we ensure more than 1 page of data exists at all times for any given stream.
        - Verify by pks that the data replicated matches the data we expect.
        """

        # Need to upgrade the mailchimp plan for collecting 'automations' stream data. Hence, skipping the stream for now.
        streams_to_skip = {"automations"}

        streams_with_2_page_size = {
            "lists",
            "list_segments",
            "campaigns",
            "unsubscribes",
        }
        streams_with_250_page_size = {"list_members", "list_segment_members"}
        streams_with_1000_page_size = {"reports_email_activity"}

        # Verify all the streams are either skipped or tested
        self.assertEqual(
            self.expected_streams() - streams_to_skip,
            streams_with_2_page_size | streams_with_250_page_size | streams_with_1000_page_size,
        )

        self.run_test(streams_with_2_page_size, 2)

        self.run_test(streams_with_250_page_size, 250)

        self.run_test(streams_with_1000_page_size, 1000)

    def run_test(self, streams, page_size):

        """Running the tap with different page size."""
        self.page_size = page_size
        expected_streams = streams
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_all_fields = [
            catalog for catalog in found_catalogs if catalog.get("tap_stream_id") in expected_streams
        ]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs_all_fields)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())

        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_primary_keys = self.expected_primary_keys()[stream]

                # Collect information for assertions from syncs 1 & 2 based on expected values
                record_count_sync = record_count_by_stream.get(stream, 0)
                primary_keys_list = [
                    tuple(message.get("data").get(expected_pk) for expected_pk in expected_primary_keys)
                    for message in synced_records.get(stream).get("messages")
                    if message.get("action") == "upsert"
                ]

                # Verify that we can paginate with all fields selected
                record_count_sync = record_count_by_stream.get(stream, 0)
                self.assertGreater(
                    record_count_sync,
                    self.page_size,
                    msg="The number of records is not over the stream max limit",
                )

                # Chunk the replicated records (just primary keys) into expected pages
                pages = []

                page_count = ceil(len(primary_keys_list) / self.page_size)
                for page_index in range(page_count):
                    page_start = page_index * self.page_size
                    page_end = (page_index + 1) * self.page_size
                    pages.append(set(primary_keys_list[page_start:page_end]))

                # Verify by primary keys that data is unique for each page
                for current_index, current_page in enumerate(pages):
                    with self.subTest(current_page_primary_keys=current_page):

                        for other_index, other_page in enumerate(pages):
                            if current_index == other_index:
                                continue  # Don't compare the page to itself

                            self.assertTrue(
                                current_page.isdisjoint(other_page),
                                msg=f"other_page_primary_keys={other_page}",
                            )
