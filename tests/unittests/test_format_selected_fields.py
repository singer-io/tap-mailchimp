import singer
from tap_mailchimp.streams import Automations
import unittest

class TestFormatSelectedFields(unittest.TestCase):
    
    def test_format_selected_fields(self):
        expected = 'automations.field1,automations.field2,_links,total_items,constraints,automations._links'
        catalog = singer.Catalog.from_dict(
            {"streams": [
                {"tap_stream_id": "automations",
                 "schema": {"properties": {"field1" : {},
                                           "field2": {}}},
                 "metadata": [
                     {"breadcrumb": [],
                      "metadata": {
                          "selected": True,
                          "inclusion": "available"
                      }},
                     {"breadcrumb": ["properties", "field1"],
                      "metadata": {
                          "selected": True,
                          "inclusion": "available"
                      }},
                     {"breadcrumb": ["properties", "field2"],
                      "metadata": {
                          "inclusion": "automatic"
                      }},
                     {"breadcrumb": ["properties", "field3"],
                      "metadata": {
                          "selected": False,
                          "inclusion": "available"
                      }},
                 ]
                }
            ]}
        )
        stream = Automations({}, {}, {}, catalog, [], [])
        actual = stream.format_selected_fields()
        self.assertEqual(','.join(sorted(actual.split(','))), ','.join(sorted(expected.split(','))))

