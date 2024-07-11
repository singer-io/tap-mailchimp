# Changelog

## 1.3.1
  * Fix the JSONDecodeError exception [#66](https://github.com/singer-io/tap-mailchimp/pull/66)

## 1.3.0
  * Updates to run on python 3.11.7 [#65](https://github.com/singer-io/tap-mailchimp/pull/65)

## 1.2.1
  * Fixed Date Window Validation Error [#62](https://github.com/singer-io/tap-mailchimp/pull/62)

## 1.2.0
  * Date Window Implementation for reports_email_activity [#61](https://github.com/singer-io/tap-mailchimp/pull/61)

## 1 1 3
  * Request Timeout Implementation [#43](https://github.com/singer-io/tap-mailchimp/pull/43)
## 1.1.2
  * Update error message when campaign export jobs exceed the timeout to indicate it is not actually an error [#38](https://github.com/singer-io/tap-mailchimp/pull/38)

## 1.1.1
  * Add 'last_changed' as a replication key to stream 'list_members' [#31](https://github.com/singer-io/tap-mailchimp/pull/31)

## 1.1.0
  * Filter fields on the request, rather than after the response is returned [#29](https://github.com/singer-io/tap-mailchimp/pull/29)

## 1.0.3
  * Fix resume batch logic [#27](https://github.com/singer-io/tap-mailchimp/pull/27)

## 1.0.2
  * Check for current bookmark in `reports_email_activity` stream [#25](https://github.com/singer-io/tap-mailchimp/pull/25)

## 1.0.1
  * Request chunks of campaigns for email_activty reports [#21](https://github.com/singer-io/tap-mailchimp/pull/21)

## 1.0.0
  * Add `list_id` as a primary key for the `list_members` stream
  * Clean up logging

## 0.1.1
  * Improve logging to make the `reports_email_activity` sync more traceable [#18](https://github.com/singer-io/tap-mailchimp/pull/18)

## 0.1.0
  * Add `page_size` to config options to control number of records per request [#12](https://github.com/singer-io/tap-mailchimp/pull/12)

## 0.0.4
  * Raise a ClientRateLimitError when Mailchimp returns a 429 so it can run backoff

## 0.0.3
 * Use get to access dictionary [commit](https://github.com/singer-io/tap-mailchimp/commit/56dfb08eba92031cff1fb5c06237a2b00d1671d6)
