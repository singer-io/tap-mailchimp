# tap-mailchimp

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Mailchimp's API](https://developer.mailchimp.com/documentation/mailchimp/reference/overview/)
- Extracts the following resources from Mailchimp:
    - [`lists`](https://developer.mailchimp.com/documentation/mailchimp/reference/lists/#read-get_lists)
    - [`list_members`](https://developer.mailchimp.com/documentation/mailchimp/reference/lists/members/#read-get_lists_list_id_members)
    - [`list_segments`](https://developer.mailchimp.com/documentation/mailchimp/reference/lists/segments/#read-get_lists_list_id_segments)
    - [`list_segment_members`](https://developer.mailchimp.com/documentation/mailchimp/reference/lists/segments/members/#read-get_lists_list_id_segments_segment_id_members)
    - [`campaigns`](https://developer.mailchimp.com/documentation/mailchimp/reference/campaigns/#read-get_campaigns)
    - [`reports_email_activity`](https://developer.mailchimp.com/documentation/mailchimp/reference/reports/email-activity/#read-get_reports_campaign_id_email_activity)
        - Uses the Mailchimp [batch API](https://developer.mailchimp.com/documentation/mailchimp/guides/how-to-use-batch-operations/)
    - [`unsubscribes`](https://developer.mailchimp.com/documentation/mailchimp/reference/reports/unsubscribed/#read-get_reports_campaign_id_unsubscribed)
    - [`automations`](https://developer.mailchimp.com/documentation/mailchimp/reference/automations/#read-get_automations)
- Outputs the schema for each resource

## Configuration

This tap requires a `config.json` which specifies details regarding authentication and other options. **`access_token` or (`api_key` and `dc`) are required**

Config properties:

| Property | Required | Example | Description |
| -------- | -------- | ------- | ----------- |
| `access_token` | See note. | "20208d81..." | The access token from the OAuth2 flow. |
| `api_key` | See note. | "ac0ad1..." | The Mailchimp API key, if using API key auth instead of OAuth. |
| `dc` | See note. | "us14" | The Mailchimp data center, only requried when using API key auth. |
| `start_date` | Y | "2010-01-01T00:00:00Z" | The default start date to use for date modified replication, when available. |
| `user_agent` | N | "Vandelay Industries ETL Runner" | The user agent to send on every request. |
| `request_timeout` | N | 300 | Time for which request should wait to get response. |

## Usage 

To run `tap-mailchimp` with the configuration file, use this command:

```sh
tap-mailchimp -c my-config.json
```

---
Copyright &copy; 2019 Stitch
