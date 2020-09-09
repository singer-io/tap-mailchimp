import singer
from tap_mailchimp.sync import format_selected_fields
import unittest

class TestFormatSelectedFields(unittest.TestCase):
    
    def test_format_selected_fields(self):
        expected = 'key1.field1,key1.field2,_links,total_items,constraints,key1._links'
        catalog = singer.Catalog.from_dict(
            {"streams": [
                {"tap_stream_id": "stream1",
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
        actual = format_selected_fields(catalog, 'stream1', 'key1')
        self.assertEqual(expected, actual)

