# Changelog

## 0.9.0
  * Add {merged_at, closed_at, assignees, time_estimate, total_time_spent, human_time_estimate, human_total_time_spent} to Merge Requests.
  * Add {assignees, time_estimate, total_time_spent, human_time_estimate, human_total_time_spent} to Issues.
  * Add support for extracting the commits of a Merge Request and storing them to table `merge_request_commits`.
  * Add `fetch_merge_request_commits` option to config. If fetch_merge_request_commits is true (defaults to false), then for each Merge Request, also fetch the MR's commits and create the join table `merge_request_commits` with the Merge Request and related Commit IDs.

## 0.8.0
  * Add support for incremental extraction of Commits, Issues, Merge Requests and Epics.
  * Properly use STATE and the `start_date` to only fetch entities created/updated after that date. 
    (tap-gitlab was fetching everything and filtering the results afterwards, which resulted in huge overhead for large projects)
  * Add dedicated STATE for commits, issues and merge_requests per Project and for epics per Group.
  * Ensure that the last message emitted is the final STATE.


## 0.7.1
  * Fix the pagination not working for very large projects with more than 10,000 entities per response.
  * Use the `X-Next-Page` header instead of the `X-Total-Pages` header.
    https://docs.gitlab.com/ee/api/#other-pagination-headers
  * Use the `per_page` param to fetch 100 records per call instead of 20. 
    No more need for 5K calls to fetch all the gitlab-ce commits. A win for all.
  * Explicitly set the per_page param to 20 for labels API end points until gitlab-org/gitlab-ce#63103 is fixed.

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
