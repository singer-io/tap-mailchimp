import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import unittest
from functools import reduce

class MailchimpBookmarks(unittest.TestCase):
    def setUp(self):
        missing_envs = [x for x in [
            "TAP_MAILCHIMP_CLIENT_ID",
            "TAP_MAILCHIMP_CLIENT_SECRET",
            "TAP_MAILCHIMP_ACCESS_TOKEN",
        ] if os.getenv(x) == None]
        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    def name(self):
        return "tap_tester_mailchimp_discovery"

    def get_type(self):
        return "platform.mailchimp"

    def get_credentials(self):
        return {
            'client_id': os.getenv('TAP_MAILCHIMP_CLIENT_ID'),
            'client_secret': os.getenv('TAP_MAILCHIMP_CLIENT_SECRET'),
            'access_token': os.getenv('TAP_MAILCHIMP_ACCESS_TOKEN')
        }

    def expected_check_streams(self):
        return {
            'automations',
            'campaigns',
            'list_members',
            'list_segment_members',
            'list_segments',
            'lists',
            'reports_email_activity',
            'unsubscribes'
        }

    def tap_name(self):
        return "tap-mailchimp"


    def get_properties(self):
        return {
            'start_date' : '2019-09-01T00:00:00Z'
        }


    def expected_sync_streams(self):
        """This is a map from stream name to the automatic fields"""
        return {
            'campaigns': {'id'},
            'list_segment_members': {'id'},
            'list_segments': {'id'},
            'lists': {'id'},
            'reports_email_activity': {'campaign_id', 'action', 'email_id', 'timestamp'},
            'unsubscribes': {'campaign_id', 'email_id'}
        }

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        diff = self.expected_check_streams().symmetric_difference( found_catalog_names )
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are OK")

        #select all catalogs

        for c in found_catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, c['stream_id'])
            if c['stream_name'] in self.expected_sync_streams().keys():
                stream = c['stream_name']
                pks = self.expected_sync_streams()[stream]

                for pk in pks:
                    mdata = next((m for m in catalog_entry['metadata']
                                  if len(m['breadcrumb']) == 2 and m['breadcrumb'][1] == pk), None)
                    print("Validating inclusion on {}: {}".format(c['stream_name'], mdata))
                    self.assertTrue(mdata and mdata['metadata']['inclusion'] == 'automatic')

                connections.select_catalog_and_fields_via_metadata(conn_id, c, catalog_entry)

        #clear state
        menagerie.set_state(conn_id, {})

        sync_job_name = runner.run_sync_mode(self, conn_id)

        #verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        first_record_count_by_stream = runner.examine_target_output_file(self, conn_id, set(self.expected_sync_streams().keys()), self.expected_sync_streams())
        replicated_row_count =  reduce(lambda accum,c : accum + c, first_record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(first_record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))

        # Verify that automatic fields are all emitted with records
        synced_records = runner.get_records_from_target_output()
        for stream_name, data in synced_records.items():
            record_messages = [set(row['data'].keys()) for row in data['messages']]
            self.assertGreater(len(record_messages), 0, msg="stream {} did not sync any records.".format(stream_name))
            for record_keys in record_messages:
                self.assertEqual(self.expected_sync_streams().get(stream_name, set()) - record_keys, set())
