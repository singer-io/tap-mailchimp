import re

from base import MailchimpBaseTest
from tap_tester import connections, menagerie


class MailchimpDiscover(MailchimpBaseTest):
    """Test tap discover mode and metadata conforms to standards."""

    def name(self):
        """Returns name of the test."""
        return "tap_tester_mailchimp_discover_test"

    def test_run(self):
        """
        - Verify number of actual streams discovered match expected
        - Verify stream names follow naming convention (streams should only have lowercase alphas and underscores_
        - verify there is only 1 top level breadcrumb
        - Verify there are no duplicate/conflicting metadata entries.
        - verify replication key(s) match expectations.
        - verify primary key(s) match expectations.
        - Verify that if there is a replication key we are doing INCREMENTAL otherwise FULL.
        - verify the actual replication matches our expected replication method
        - Verify all streams have inclusion of automatic
        - verify that primary, replication are given the inclusion of automatic
        - verify that all other fields have inclusion of available (metadata and schema)
        """

        streams_to_test = self.expected_streams()

        conn_id = connections.ensure_connection(self, payload_hook=None)

        # Verify that there are catalogs found
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        found_catalog_names = {c["tap_stream_id"] for c in found_catalogs}

        # Verify number of actual streams discovered match expected
        self.assertEqual(
            set(streams_to_test), set(found_catalog_names), msg="Expected streams don't match actual streams"
        )

        # Verify stream names follow the naming convention
        # Streams should only have lowercase alphas and underscores
        self.assertTrue(
            all([re.fullmatch(r"[a-z_]+", name) for name in found_catalog_names]),
            msg="One or more streams don't follow standard naming",
        )

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Verify the catalog is found for a given stream
                catalog = next(iter([catalog for catalog in found_catalogs if catalog["stream_name"] == stream]))
                self.assertIsNotNone(catalog)

                # Collecting expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_automatic_fields = self.expected_automatic_fields().get(stream)
                expected_replication_method = self.expected_replication_method()[stream]

                # Collecting actual values...
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])
                metadata = schema_and_metadata["metadata"]

                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]

                actual_primary_keys = set(
                    stream_properties[0].get("metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, [])
                )
                actual_replication_keys = set(
                    stream_properties[0].get("metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, [])
                )
                actual_replication_method = (
                    stream_properties[0].get("metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)
                )

                actual_automatic_fields = {
                    item.get("breadcrumb", ["properties", None])[1]
                    for item in metadata
                    if item.get("metadata").get("inclusion") == "automatic"
                }

                actual_fields = []
                for md_entry in metadata:
                    if md_entry["breadcrumb"] != []:
                        actual_fields.append(md_entry["breadcrumb"][1])

                ##########################################################################
                # metadata assertions
                ##########################################################################

                # verify there is only 1 top level breadcrumb in metadata
                self.assertTrue(
                    len(stream_properties) == 1,
                    msg=f"There is NOT only one top level breadcrumb for {stream}"
                    + f"\nstream_properties | {stream_properties}",
                )

                # Verify there are no duplicate metadata entries
                self.assertEqual(
                    len(actual_fields),
                    len(set(actual_fields)),
                    msg=f"duplicates in the fields retrieved",
                )

                # Verify the primary key(s) match expectations
                self.assertSetEqual(
                    expected_primary_keys,
                    actual_primary_keys,
                )

                # Verify that primary keys and replication keys
                # are given the inclusion of automatic in metadata.
                self.assertSetEqual(expected_automatic_fields, actual_automatic_fields)

                # Verify that all other fields have an inclusion of available metadata
                # This assumes there are no unsupported fields for SaaS sources
                self.assertTrue(
                    all(
                        {
                            item.get("metadata").get("inclusion") == "available"
                            for item in metadata
                            if item.get("breadcrumb", []) != []
                            and item.get("breadcrumb", ["properties", None])[1] not in actual_automatic_fields
                        }
                    ),
                    msg="Not all non key properties are set to available in metadata",
                )

                # Verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                if actual_replication_keys:
                    self.assertTrue(
                        actual_replication_method == self.INCREMENTAL,
                        msg="Expected INCREMENTAL replication " "since there is a replication key",
                    )
                else:
                    self.assertTrue(
                        actual_replication_method == self.FULL_TABLE,
                        msg="Expected FULL replication " "since there is no replication key",
                    )

                # Verify the actual replication matches our expected replication method
                self.assertEqual(
                    expected_replication_method,
                    actual_replication_method,
                    msg="The actual replication method {} doesn't match the expected {}".format(
                        actual_replication_method, expected_replication_method
                    ),
                )

                # Verify replication key(s) match expectations
                self.assertEqual(
                    expected_replication_keys,
                    actual_replication_keys,
                    msg="expected replication key {} but actual is {}".format(
                        expected_replication_keys, actual_replication_keys
                    ),
                )

                # verify that all other fields have inclusion of available
                # This assumes there are no unsupported fields for SaaS sources
                self.assertTrue(
                    all(
                        {
                            item.get("metadata").get("inclusion") == "available"
                            for item in metadata
                            if item.get("breadcrumb", []) != []
                            and item.get("breadcrumb", ["properties", None])[1] not in actual_automatic_fields
                        }
                    ),
                    msg="Not all non key properties are set to available in metadata",
                )
