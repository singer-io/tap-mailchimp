import singer
from tap_mailchimp.client import MailchimpClient
from tap_mailchimp.streams import ReportEmailActivity
import unittest


class TestFormatSelectedFields(unittest.TestCase):

    def test_format_selected_fields(self):
        expected = 'emails.field1,emails.field2,_links,total_items,constraints,emails._links,emails.activity'
        catalog = singer.Catalog.from_dict(
            {
                "streams": [
                    {
                        "tap_stream_id": "reports_email_activity",
                        "schema": {
                            "properties": {
                                "field1": {},
                                "field2": {}
                            }
                        },
                        "metadata": [
                            {
                                "breadcrumb": [],
                                "metadata": {
                                    "selected": True,
                                    "inclusion": "available"
                                }
                            },
                            {
                                "breadcrumb": [
                                    "properties",
                                    "field1"
                                ],
                                "metadata": {
                                    "selected": True,
                                    "inclusion": "available"
                                }
                            },
                            {
                                "breadcrumb": [
                                    "properties",
                                    "field2"
                                ],
                                "metadata": {
                                    "inclusion": "automatic"
                                }
                            },
                            {
                                "breadcrumb": [
                                    "properties",
                                    "field3"
                                ],
                                "metadata": {
                                    "selected": False,
                                    "inclusion": "available"
                                }
                            },
                        ]
                    }
                ]
            }
        )

        client = MailchimpClient({})
        stream = ReportEmailActivity(
            state={},
            client=client,
            config={},
            catalog=catalog,
            selected_stream_names=[],
            child_streams_to_sync=[]
        )

        actual = stream.format_selected_fields()

        self.assertEqual(','.join(sorted(actual.split(','))),
                         ','.join(sorted(expected.split(','))))
