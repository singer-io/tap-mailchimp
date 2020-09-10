import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import unittest

class TestMailchimpDiscovery(unittest.TestCase):
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
        """This is a map from stream name to the automatic fields"""
        return {
            'campaigns': {'id'},
            'list_segment_members': {'id'},
            'list_segments': {'id'},
            'lists': {'id'},
            'reports_email_activity': {'campaign_id', 'action', 'email_id', 'timestamp'},
            'unsubscribes': {'campaign_id', 'email_id'},
            'automations': {'id'},
            'list_members': {'id', 'list_id', 'last_changed'}
        }

    def tap_name(self):
        return "tap-mailchimp"


    def get_properties(self):
        return {
            'start_date' : '2019-09-01T00:00:00Z'
        }

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        diff = set(self.expected_check_streams().keys()).symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are OK")

        for catalog in found_catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            stream = catalog['stream_name']
            automatic_fields = self.expected_check_streams()[stream]

            for field in automatic_fields:
                mdata = next((m for m in catalog_entry['metadata']
                              if len(m['breadcrumb']) == 2 and m['breadcrumb'][1] == field), None)
                print("Validating inclusion on {}: {}".format(catalog['stream_name'], mdata))
                self.assertTrue(mdata and mdata['metadata']['inclusion'] == 'automatic')
