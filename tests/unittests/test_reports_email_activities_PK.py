import hashlib
import unittest
from tap_mailchimp.streams import ReportEmailActivity
from tap_mailchimp.client import MailchimpClient
from test_sync import Catalog


class TestReportsEmailActivitiesPrimaryKey(unittest.TestCase):
    """Test case to verify we generate '_sdc_record_hash' for reports_email_activity"""

    def test_reports_email_activities_PK(self):
        # List of records
        records = [
            {
                "campaign_id": 1,
                "email_id": "f12345abcd",
                "activity": [
                    {
                        "action": "open",
                        "timestamp": "2022-01-01T10:15:22+00:00",
                        "ip": "10.0.0.1"
                    },
                    {
                        "action": "bounce",
                        "timestamp": "2022-01-02T19:11:48+00:00"
                    }
                ]
            }
        ]

        # Mailchimp client
        client = MailchimpClient(config={})

        # 'reports_email_activity' stream object
        stream = ReportEmailActivity(
            state={},
            client=client,
            config={},
            catalog=Catalog("reports_email_activity"),
            selected_stream_names=["reports_email_activity"],
            child_streams_to_sync=[]
        )

        # Function call
        transformed_records = list(stream.transform_activities(records=records))

        # Create expected data by hashing the string of key-value pairs for ["campaign_id", "action", "email_id", "timestamp", "ip"] fields
        expected_data = [
            hashlib.sha256(
                "campaign_id1actionopenemail_idf12345abcdtimestamp2022-01-01T10:15:22+00:00ip10.0.0.1".encode("utf-8")).hexdigest(),
            hashlib.sha256(
                "campaign_id1actionbounceemail_idf12345abcdtimestamp2022-01-02T19:11:48+00:00ip".encode("utf-8")).hexdigest()
        ]
        # Verify we got transformed records
        self.assertIsNotNone(transformed_records)

        # Verify '_sdc_records_hash' is present for all records and compare the hash with expected hash
        for actual_record, expected_record in list(zip(transformed_records, expected_data)):
            actual_record_hash = actual_record.get("_sdc_record_hash")
            self.assertIsNotNone(actual_record_hash)
            self.assertEqual(actual_record_hash, expected_record)
