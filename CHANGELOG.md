# Changelog

## 0.1.0
  * Add `page_size` to config options to control number of records per request [#12](https://github.com/singer-io/tap-mailchimp/pull/12)

## 0.0.4
  * Raise a ClientRateLimitError when Mailchimp returns a 429 so it can run backoff

## 0.0.3
 * Use get to access dictionary [commit](https://github.com/singer-io/tap-mailchimp/commit/56dfb08eba92031cff1fb5c06237a2b00d1671d6)
