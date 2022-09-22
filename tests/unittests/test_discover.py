import unittest
from singer.catalog import Catalog
from tap_mailchimp.discover import discover


class DiscoverTest(unittest.TestCase):
    '''
        Test class to verify proper working of discover mode function.
    '''

    def test_discover(self):
        '''
            Test case to verify catalog is generated correctly.
        '''

        returned_catalog = discover()
        self.assertEqual(type(returned_catalog), Catalog)
