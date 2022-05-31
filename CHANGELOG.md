# Changelog

## 2.0.0

- This release is a full rewrite on the Meltano [SDK](https://sdk.meltano.com) for Singer Taps.
- License changed to Apache 2.0.

## 0.9.15

  * [#39](https://gitlab.com/meltano/tap-gitlab/-/issues/39) Add support for self-signed SSL certificate on self-hosted GitLab instance by allowing a certificate bundle to be configured using the `REQUESTS_CA_BUNDLE` env var.

## 0.9.14
  * [!37](https://gitlab.com/meltano/tap-gitlab/-/merge_requests/37) Refactor `jobs` syncing to increase performance

## 0.9.13
  * [#35](https://gitlab.com/meltano/tap-gitlab/-/issues/35) Select all streams when no catalog is provided, to ensure backward compatibility
  * [#36](https://gitlab.com/meltano/tap-gitlab/-/issues/36) Make all properties nullable to pervent targets from failing when properties are deselected and not included in records

## 0.9.12
  * [#25](https://gitlab.com/meltano/tap-gitlab/-/issues/25) Implement discovery mode and stream/field selection

## 0.9.11
  * [#33](https://gitlab.com/meltano/tap-gitlab/-/issues/33) Fix support for `start_date`s without a timestamp or timezone by assuming UTC

## 0.9.10
  * [#31](https://gitlab.com/meltano/tap-gitlab/-/issues/31) `start_date` takes precedence over state timestamp when it is more recent

## 0.9.9
  * [#28](https://gitlab.com/meltano/tap-gitlab/-/issues/28) Add `jobs` resource

## 0.9.8
  * [#26](https://gitlab.com/meltano/tap-gitlab/-/issues/26) Fix bug that caused projects not to be synced when group(s) were provided along with project path(s) instead of project ID(s)

## 0.9.7
  * [#24](https://gitlab.com/meltano/tap-gitlab/-/issues/24) Set a default value for `fetch_pipeline_extended`

## 0.9.6
  [!23](https://gitlab.com/meltano/tap-gitlab/-/merge_requests/23)
  * Add `pipelines` endpoint (https://docs.gitlab.com/ee/api/pipelines.html#list-project-pipelines)
  * Add `pipelines_extended` endpoint (https://docs.gitlab.com/ee/api/pipelines.html#get-a-single-pipeline)
  * Make `gen_request` function handle requests that return a single JSON object, instead of an array of objects
  * Add `stats__*` columns to `commits` endpoint
  * Add `released_at` column to `releases` endpoint
  ([Tomasz Zbrozek](https://gitlab.com/tomekzbrozek))

## 0.9.5
  * [!22](https://gitlab.com/meltano/tap-gitlab/-/merge_requests/22) Fix bug causing only projects in first group to be synced if multiple groups are specified ([Tomasz Zbrozek](https://gitlab.com/tomekzbrozek))

## 0.9.4
  * [#21](https://gitlab.com/meltano/tap-gitlab/issues/21) Stop Tap's execution with an error if a request for a resource returns an HTTP 404 response. That means that the resource (e.g. a group or a project) is not there and most probably indicates a user error with setting up `tap-gitlab`

## 0.9.3
  * [#16](https://gitlab.com/meltano/tap-gitlab/issues/17) Remove requirement to have the api version hard-coded on the `api_url` parameter. The `api_url` now requires only the base URL of the GitLab instance, e.g. `https://gitlab.com`. Old configuration settings or manually setting the version are still supported.

## 0.9.2
  * [#16](https://gitlab.com/meltano/tap-gitlab/issues/16) Handle 401 (Unauthorized), 403 (Forbidden) and 404 (Not Found) Resource errors gracefully: Skip extracting that resource and continue with the rest. That can happen, for example, when accessing a private project or accessing the members, milestones or labels of a project without sufficient privileges.

## 0.9.1
  * Update Issues to also fetch the closed_by_id attribute.
  * Always load all extracted milestones, not only the ones updated after start_date.

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
