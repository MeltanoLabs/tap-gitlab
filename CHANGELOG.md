# Changelog

## 0.7.0
  * Update config options to allow for Gitlab Ultimate and Gitlab.com Gold account features
  * Add support for fetching Epics and Epic Issues for Gitlab Ultimate and Gitlab.com Gold accounts

## 0.6.3
  * Fetch additional {'name', 'username', 'expires_at', 'state'} attributes for Group Members
  * Fetch additional {'closed_at', 'discussion_locked', 'has_tasks', 'task_status'} attributes for Issues

## 0.6.2
  * Upgrade singer-python to 5.6.1
  * Add support for the --discover flag

## 0.6.1
  * Set GitLab Private-Token in headers, so that it is not logged when tap runs

## 0.6.0
  * Add support for fetching Group and Project Labels
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
