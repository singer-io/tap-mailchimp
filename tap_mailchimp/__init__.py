#!/usr/bin/env python3

import json
import sys

import singer

from tap_mailchimp.client import MailchimpClient
from tap_mailchimp.discover import discover
from tap_mailchimp.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date"]


def do_discover(client):
    LOGGER.info("Testing authentication")
    try:
        client.get("/lists", params={"count": 1})
    except Exception as e:
        msg = f"Error testing Mailchimp authentication. Error: {e.__class__.__name__}: {str(e)}"
        raise Exception(msg) from None

    LOGGER.info("Starting discover")
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Finished discover")


@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with MailchimpClient(parsed_args.config) as client:
        if parsed_args.discover:
            do_discover(client)
        elif parsed_args.catalog:
            sync(client, parsed_args.catalog, parsed_args.state, parsed_args.config)
