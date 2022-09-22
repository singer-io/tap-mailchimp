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

        # Verify we got transformed records
        self.assertIsNotNone(transformed_records)

        # Verify '_sdc_records_hash' is present for all records
        for record in transformed_records:
            self.assertIsNotNone(record.get("_sdc_record_hash"))
