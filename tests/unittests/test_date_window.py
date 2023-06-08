import unittest
from datetime import datetime,timedelta
from tap_mailchimp.client import MailchimpClient

class TestDateWindowConfig(unittest.TestCase):
    
    def test_datewindow_disabled_no_val(self):
        """
            Verify if date_windowing is disabled if no value is passed
        """
        # Initialize MailchimpClient object
        client = MailchimpClient({'access_token': 'TOKEN'})
        
        self.assertEqual(client.adjusted_start_date,False)
    
    def test_datewindow_disabled_empty_str(self):
        """
            Verify if date_windowing is disabled if empty string value is passed
            Verify no Exception is raised for typecasting error between str to num
        """
        # Initialize MailchimpClient object
        client = MailchimpClient({'access_token': 'TOKEN',"date_window":""})
        
        self.assertEqual(client.adjusted_start_date,False)

    def test_datewindow_disabled_bool_val(self):
        """
            Verify if date_windowing is disabled if bool value is passed
        """
        # Initialize MailchimpClient object
        client = MailchimpClient({'access_token': 'TOKEN',"email_activity_date_window":False})
        self.assertEqual(client.adjusted_start_date,False)

    def test_datewindow_disabled_num_val(self):
        """
            Verify if date_window is disabled if 0 value is passed
        """
        # Initialize MailchimpClient object
        client = MailchimpClient({'access_token': 'TOKEN',"email_activity_date_window":0})
        self.assertEqual(client.adjusted_start_date,False)

    def test_datewindow_enabled_num_val(self):
        """
            Verify if date_window is enabled if num value is passed
        """
        # Initialize MailchimpClient object
        client = MailchimpClient({'access_token': 'TOKEN',"email_activity_date_window":3})
        
        time_diff = datetime.now().date() - client.adjusted_start_date

        self.assertEqual(timedelta(days=3),time_diff)
