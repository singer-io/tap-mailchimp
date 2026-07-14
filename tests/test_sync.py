"""
Basic sync test for tap-mailchimp.

Verifies:
  - Discovery returns all expected streams.
  - Every key property on every selected stream has inclusion=automatic.
  - A full sync emits at least one record across all selected streams.
  - Every record contains all automatic fields (primary keys + replication keys).
"""
from functools import reduce

from base import MailchimpBaseTest
from tap_tester import connections, menagerie, runner


class MailchimpSyncTest(MailchimpBaseTest):

    @staticmethod
    def name():
        return "tap_tester_mailchimp_sync_test"

    def streams_to_test(self):
        return self.expected_stream_names()

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # ---- discovery / check mode ---------------------------------- #
        check_job_name = runner.run_check_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(
            len(found_catalogs), 0,
            msg="unable to locate schemas for connection {}".format(conn_id),
        )

        found_catalog_names = {c["tap_stream_id"] for c in found_catalogs}
        diff = self.streams_to_test().symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are OK")

        # ---- catalog selection & automatic-field validation ---------- #
        expected_auto = self.expected_automatic_fields()

        for c in found_catalogs:
            if c["stream_name"] not in self.streams_to_test():
                continue

            catalog_entry = menagerie.get_annotated_schema(conn_id, c["stream_id"])
            stream = c["stream_name"]

            for field in expected_auto.get(stream, set()):
                mdata = next(
                    (m for m in catalog_entry["metadata"]
                     if len(m["breadcrumb"]) == 2 and m["breadcrumb"][1] == field),
                    None,
                )
                print("Validating inclusion on {}: {}".format(stream, mdata))
                self.assertTrue(
                    mdata and mdata["metadata"]["inclusion"] == "automatic",
                    msg="Field '{}' on stream '{}' should have inclusion=automatic".format(
                        field, stream
                    ),
                )

            connections.select_catalog_and_fields_via_metadata(conn_id, c, catalog_entry)

        # ---- sync run ------------------------------------------------ #
        menagerie.set_state(conn_id, {})
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id,
            self.streams_to_test(),
            self.expected_primary_keys(),
        )
        total_records = reduce(lambda a, c: a + c, record_count_by_stream.values())
        self.assertGreater(
            total_records, 0,
            msg="failed to replicate any data: {}".format(record_count_by_stream),
        )
        print("total replicated row count: {}".format(total_records))

        # ---- every record must contain all automatic fields ---------- #
        synced_records = runner.get_records_from_target_output()
        for stream_name, data in synced_records.items():
            record_messages = [set(row["data"].keys()) for row in data["messages"]]
            self.assertGreater(
                len(record_messages), 0,
                msg="stream {} did not sync any records.".format(stream_name),
            )
            auto_fields = expected_auto.get(stream_name, set())
            for record_keys in record_messages:
                self.assertEqual(
                    auto_fields - record_keys,
                    set(),
                    msg="stream '{}' record missing automatic fields: {}".format(
                        stream_name, auto_fields - record_keys
                    ),
                )

