import unittest
import os
from tap_tester import menagerie, runner, connections, LOGGER
from datetime import datetime as dt


class MailchimpBaseTest(unittest.TestCase):
    """
    Setup expectations for test sub classes.
    Metadata describing streams.
    A bunch of shared methods that are used in tap-tester tests.
    Shared tap-specific methods (as needed).
    """

    start_date = ""
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    REPLICATION_KEYS = "valid-replication-keys"
    FULL_TABLE = "FULL_TABLE"
    INCREMENTAL = "INCREMENTAL"
    OBEYS_START_DATE = "obey-start-date"
    BOOKMARK_PATH = "bookmark-path"
    EXTRA_AUTOMATIC_FIELDS = "extra-automatic-fields"
    BOOKMARK_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.000000Z"
    PARENT = "parent-stream"

    def tap_name(self):
        """The name of the tap"""
        return "tap-mailchimp"

    def setUp(self):
        """Raising the error if required environment variables are missing"""
        required_env = {
            "TAP_MAILCHIMP_CLIENT_SECRET",
            "TAP_MAILCHIMP_CLIENT_ID",
            "TAP_MAILCHIMP_ACCESS_TOKEN",
        }
        missing_envs = [v for v in required_env if not os.getenv(v)]
        if missing_envs:
            raise Exception("set " + ", ".join(missing_envs))

    def get_type(self):
        """The expected url route ending"""
        return "platform.mailchimp"

    def get_credentials(self):
        """Authentication information for the test account"""
        return {
            "client_id": os.getenv("TAP_MAILCHIMP_CLIENT_ID"),
            "client_secret": os.getenv("TAP_MAILCHIMP_CLIENT_SECRET"),
            "access_token": os.getenv("TAP_MAILCHIMP_ACCESS_TOKEN"),
        }

    def get_properties(self, original=True):
        """Configuration properties required for the tap."""
        return_value = {
            "start_date": "2013-01-01T00:00:00Z",
        }
        if original:
            return return_value

        # Reassign start date
        return_value["start_date"] = self.start_date
        return return_value

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""
        return {
            "automations": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: False,
                self.BOOKMARK_PATH: None,
                self.PARENT: None
            },
            "campaigns": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: False,
                self.BOOKMARK_PATH: None,
                self.PARENT: None
            },
            "lists": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: False,
                self.BOOKMARK_PATH: None,
                self.PARENT: None
            },
            "list_members": {
                self.PRIMARY_KEYS: {"id", "list_id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"last_changed"},
                self.OBEYS_START_DATE: True,
                self.BOOKMARK_PATH: ["lists", "8c775a04fb", "list_members", "datetime"],
                self.PARENT: "lists"
            },
            "list_segments": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: False,
                self.BOOKMARK_PATH: None,
                self.PARENT: "lists"
            },
            "list_segment_members": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: False,
                self.BOOKMARK_PATH: None,
                self.PARENT: None
            },
            'reports_email_activity': {
                self.PRIMARY_KEYS: {'_sdc_record_hash'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"timestamp"},
                self.OBEYS_START_DATE: True,
                self.BOOKMARK_PATH: ["reports_email_activity", "32e6edcecb"],
                self.PARENT: "campaigns",
                self.EXTRA_AUTOMATIC_FIELDS: {'action', 'campaign_id', 'email_id', 'timestamp', 'ip'}
            },
            "unsubscribes": {
                self.PRIMARY_KEYS: {"campaign_id", "email_id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.OBEYS_START_DATE: True,
                self.REPLICATION_KEYS: {"timestamp"},
                self.BOOKMARK_PATH: ['unsubscribes', '5b483c58de', 'timestamp'],
                self.PARENT: "campaigns"
            },
        }

    def expected_check_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def expected_replication_keys(self):
        """Return a dictionary with the key of table name and value as a set of replication key fields"""

        return {
            table: properties.get(self.REPLICATION_KEYS, set())
            for table, properties in self.expected_metadata().items()
        }

    def expected_primary_keys(self):
        """Return a dictionary with the key of table name and value as a set of primary key fields"""
        return {
            table: properties.get(self.PRIMARY_KEYS, set())
            for table, properties in self.expected_metadata().items()
        }

    def expected_replication_method(self):
        """Return a dictionary with the key of table name nd value of replication method"""
        return {
            table: properties.get(self.REPLICATION_METHOD, set())
            for table, properties in self.expected_metadata().items()
        }

    def get_bookmark_path(self):
        """Return bookmark path (the path at which bookmark is stored) for the stream"""
        return {
            table: properties.get(self.BOOKMARK_PATH, set())
            for table, properties in self.expected_metadata().items()
        }

    def get_parent(self):
        """Return the parent stream for the stream"""
        return {
            table: properties.get(self.PARENT, set())
            for table, properties in self.expected_metadata().items()
        }

    def expected_automatic_fields(self):
        """Return a dictionary with the key of table name and set of value of automatic(primary key and bookmark field) fields"""
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) |  v.get(self.REPLICATION_KEYS, set()) | \
                v.get(self.EXTRA_AUTOMATIC_FIELDS, set())
        return auto_fields

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be run prior to field selection and initial sync.
        Return the connection id and found catalogs from the menagerie.
        """
        # Run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(
            len(found_catalogs),
            0,
            msg="unable to locate schemas for connection {}".format(conn_id),
        )

        found_catalog_names = set(map(lambda c: c["stream_name"], found_catalogs))
        self.assertSetEqual(
            self.expected_check_streams(),
            found_catalog_names,
            msg="discovered schemas do not match",
        )
        LOGGER.info("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_check_streams(), self.expected_primary_keys()
        )

        self.assertGreater(
            sum(sync_record_count.values()),
            0,
            msg="failed to replicate any data: {}".format(sync_record_count),
        )
        LOGGER.info(
            "total replicated row count: {}".format(sum(sync_record_count.values()))
        )

        return sync_record_count

    def perform_and_verify_table_and_field_selection(
        self, conn_id, test_catalogs, select_all_fields=True
    ):
        """
        Perform table and field selection based on the streams to select
        set and field selection parameters.
        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get("stream_name") for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat["stream_id"])

            # Verify all testable streams are selected
            selected = catalog_entry.get("annotated-schema").get("selected")
            LOGGER.info(
                "Validating selection on {}: {}".format(cat["stream_name"], selected)
            )
            if cat["stream_name"] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue  # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in (
                    catalog_entry.get("annotated-schema").get("properties").items()
                ):
                    field_selected = field_props.get("selected")
                    LOGGER.info(
                        "\tValidating selection on {}.{}: {}".format(
                            cat["stream_name"], field, field_selected
                        )
                    )
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(
                    cat["stream_name"]
                )
                selected_fields = self.get_selected_fields_from_metadata(
                    catalog_entry["metadata"]
                )
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        """Return set of selected fields from the metadata"""
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field["breadcrumb"]) > 1
            inclusion_automatic_or_selected = (
                field["metadata"]["selected"] is True
                or field["metadata"]["inclusion"] == "automatic"
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field["breadcrumb"][1])
        return selected_fields

    @staticmethod
    def select_all_streams_and_fields(
        conn_id, catalogs, select_all_fields: bool = True
    ):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])

            non_selected_properties = []
            if not select_all_fields:
                # Get a list of all properties so that none are selected
                non_selected_properties = (
                    schema.get("annotated-schema", {}).get("properties", {}).keys()
                )

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties
            )

    def parse_date(self, date_value, format):
        """
        Pass in string-formatted-datetime, parse the value, and return it as an unformatted datetime object.
        """
        date_stripped = dt.strptime(date_value, format)
        return date_stripped

    def get_bookmark(self, bookmark, path):
        """Return the bookmark at the specified path from the state file"""
        if not path:
            return None

        if len(path) == 1:
            return bookmark.get(path[0])

        return self.get_bookmark(bookmark.get(path[0], {}), path[1:])
