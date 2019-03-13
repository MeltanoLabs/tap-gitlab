# Changelog

## 0.6.0
  * Add support for fetching Tags and Releases
  * Add support for fetching Merge Requests
  * Add support for fetching Group and Project Members
  * Update Users with Member info 
  * Fetch additional {'default', 'can_push'} attributes for Branches
  * Fetch additional {'authored_date', 'committed_date', 'parent_ids'} attributes for Commits
  * Fetch additional {'upvotes', 'downvotes', 'merge_requests_count' 'weight'} attributes for Issues
  * Fetch additional {'merge_method', 'statistics', 'visibility'} attributes for Projects

## 0.5.1
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 0.5.0
  * Added support for groups and group milestones [#9](https://github.com/singer-io/tap-gitlab/pull/9)
