#!/usr/bin/env python3

import json
import sys
import datetime
from datetime import timezone

import isodate
import singer
from singer.catalog import Catalog
from hotglue_singer_sdk import Tap, typing as th

from tap_mailchimp.auth import MailchimpOAuthAuthenticator
from tap_mailchimp.client import MailchimpClient
from tap_mailchimp.discover import discover
from tap_mailchimp.sync import sync

LOGGER = singer.get_logger()


def resolve_start_date(config):
    """Return the effective start date from config.

    If start_date_offset is set (ISO 8601 duration, e.g. P60D), calculate the
    start date as now() minus that duration. Otherwise fall back to start_date.
    """
    offset = config.get("start_date_offset")
    if offset:
        duration = isodate.parse_duration(offset)
        return (datetime.datetime.now(timezone.utc) - duration).isoformat()
    start_date = config.get("start_date")
    if not start_date:
        raise Exception("Either start_date or start_date_offset must be set in config")
    return start_date


def do_discover(client):
    LOGGER.info('Testing authentication')
    try:
        client.get('/lists', params={'count': 1})
    except:
        raise Exception('Error testing Mailchimp authentication')

    LOGGER.info('Starting discover')
    catalog = discover(client)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


class TapMailchimp(Tap):
    """Singer tap for Mailchimp."""

    name = "tap-mailchimp"

    config_jsonschema = th.PropertiesList(
        th.Property("access_token", th.StringType, description="OAuth access token"),
        th.Property("api_key", th.StringType, description="API key"),
        th.Property("dc", th.StringType, description="Data center (e.g. us14), required with api_key auth"),
        th.Property("start_date", th.StringType, description="Start date for incremental syncs (ISO 8601)"),
        th.Property("start_date_offset", th.StringType, description="ISO 8601 duration offset from now (e.g. P60D). Overrides start_date when set."),
        th.Property("page_size", th.IntegerType, default=1000, description="Page size for API requests"),
        th.Property("user_agent", th.StringType, description="User agent for API requests"),
    ).to_dict()

    @classmethod
    def access_token_support(cls, connector=None):
        return (MailchimpOAuthAuthenticator, None)

    def discover_streams(self):
        return []

    def run_discovery(self):
        config = dict(self.config)
        with MailchimpClient(config) as client:
            do_discover(client)

    def run_sync(self, catalog=None, state=None):
        config = dict(self.config)

        if not config.get('access_token') and not config.get('api_key'):
            raise Exception('Either access_token or api_key must be set in config')

        singer_catalog = Catalog.load(catalog) if catalog else None

        if state:
            with open(state) as f:
                singer_state = json.load(f)
        else:
            singer_state = {}

        start_date = resolve_start_date(config)

        with MailchimpClient(config) as client:
            sync(client, singer_catalog, singer_state, start_date)


def main():
    TapMailchimp.cli()


if __name__ == '__main__':
    main()
