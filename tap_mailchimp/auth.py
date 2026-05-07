"""Mailchimp OAuth authenticator."""

import json

from hotglue_singer_sdk.authenticators import OAuthAuthenticator
from hotglue_singer_sdk.helpers._util import utc_now

THIRTY_DAYS_SECONDS = 30 * 24 * 60 * 60


class MailchimpOAuthAuthenticator(OAuthAuthenticator):
    """Authenticator for Mailchimp OAuth access tokens.

    Mailchimp access tokens do not expire, so we just return the token from
    config and set expires_in to 30 days so client-api knows how long to cache it.
    """

    def update_access_token_locally(self) -> None:
        request_time = utc_now()
        access_token = self.config.get("access_token")

        self.access_token = access_token
        self.expires_in = int(request_time.timestamp()) + THIRTY_DAYS_SECONDS
        self.last_refreshed = request_time

        self._tap._config["access_token"] = access_token
        self._tap._config["expires_in"] = self.expires_in

        if self._tap.config_file is not None:
            with open(self._tap.config_file, "w") as outfile:
                json.dump(self._tap._config, outfile, indent=4)
