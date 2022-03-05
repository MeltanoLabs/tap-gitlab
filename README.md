# `tap-gitlab`

GitLab tap class.

Built with the [Meltano SDK](https://sdk.meltano.com) for Singer Taps and Targets.

## Capabilities

- `catalog`
- `state`
- `discover`
- `about`
- `stream-maps`
- `schema-flattening`

## Settings

| Setting                    | Required | Default | Description |
|:---------------------------|:--------:|:-------:|:------------|
| api_url                    | False    | None    | Optionally overrides the default base URL for the Gitlab API. |
| private_token              | True     | None    | An access token to use when calling to the Gitlab API. |
| groups                     | False    | None    | A space delimited list of group ids, e.g. 'orgname1 orgname2 orgname3' |
| projects                   | False    | None    | A space delimited list of project ids, e.g. 'orgname/projectname1 orgname/projectname2 |
| start_date                 | False    | None    | Optional. If provided, this is the furthest date for which data will be retrieved. |
| ultimate_license           | False    | None    | If not set to 'true', the following streams will be ignored: 'epics' and 'epic_issues'. |
| fetch_merge_request_commits| False    | None    | If not set to 'true', the 'merge_request_commits' stream will be ignored. |
| fetch_pipelines_extended   | False    | None    | If not set to 'true', the 'pipelines_extended' stream will be ignored. |
| fetch_group_variables      | False    | None    | If not set to 'true', the 'group_variables' stream will be ignored. |
| fetch_project_variables    | False    | None    | If not set to 'true', the 'project_variables' stream will be ignored. |
| fetch_site_users           | False    | None    | Unless set to 'false', the 'site_users' stream will be included. |
| requests_cache_path        | False    | None    | (Optional.) Specifies the directory of API request caches.When this is set, the cache will be used before calling to the external API endpoint. Any data not already cached will be recorded to this path as it is received. |
| stream_maps                | False    | None    | Config object for stream maps capability. |
| stream_map_config          | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled         | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth       | False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities is available by running: `tap-gitlab --about`

### Example Settings File

`config.json`

```js
{
  "api_url": "https://gitlab.com/api/v4",   // optionally overrides the base URL
  "private_token": "your-access-token",
  "groups": "myorg mygroup",
  "projects": "myorg/repo-a myorg/repo-b",
  "start_date": "2018-01-01T00:00:00Z",
  "ultimate_license": true,
  "fetch_merge_request_commits": false,
  "fetch_pipelines_extended": false,
  "fetch_group_variables": false,
  "fetch_project_variables": false
}
```

### Additional Configuration Info

Notes on group and project options:

- either groups or projects need to be provided
- filling in 'groups' but leaving 'projects' empty will sync all group projects.
- filling in 'projects' but leaving 'groups' empty will sync selected projects.
- filling in 'groups' and 'projects' will sync selected projects of those groups.

- If `ultimate_license` is true (defaults to false), then the GitLab account used has access to the GitLab Ultimate or GitLab.com Gold features. It will enable fetching Epics, Epic Issues and other entities available for GitLab Ultimate and GitLab.com Gold accounts.

- If `fetch_merge_request_commits` is true (defaults to false), then for each Merge Request, also fetch the MR's commits and create the join table `merge_request_commits` with the Merge Request and related Commit IDs. In the current version of GitLab's API, this operation requires one API call per Merge Request, so setting this to True can slow down considerably the end-to-end extraction time. For example, in a project like `gitlab-org/gitlab-foss`, this would result to 15x more API calls than required for fetching all the other Entities supported by `tap-gitlab`.

- If `fetch_pipelines_extended` is true (defaults to false), then for every Pipeline fetched with `sync_pipelines` (which returns N pages containing all pipelines per project), also fetch extended details of each of these pipelines with `sync_pipelines_extended`. Similar concerns as those related to `fetch_merge_request_commits` apply here - every pipeline fetched with `sync_pipelines_extended` requires a separate API call.

- If `fetch_group_variables` is true (defaults to false), then Group-level CI/CD variables will be retrieved for each available / specified group. This feature is treated as an opt-in to prevent users from accidentally extracting any potential secrets stored as Group-level CI/CD variables.

- If `fetch_project_variables` is true (defaults to false), then Project-level CI/CD variables will be retrieved for each available / specified project. This feature is treated as an opt-in to prevent users from accidentally extracting any potential secrets stored as Project-level CI/CD variables.

## Installation

```bash
pipx install git+https://github.com/MeltanoLabs/tap-gitlab.git@v2.0.0-alpha1
```

Check the [releases page](https://github.com/MeltanoLabs/tap-gitlab/releases) in GitHub for the latest version number.

### Available Stream Types

- [Branches](https://docs.gitlab.com/ee/api/branches.html)
- [Commits](https://docs.gitlab.com/ee/api/commits.html)
- [Issues](https://docs.gitlab.com/ee/api/issues.html)
- [Pipelines](https://docs.gitlab.com/ee/api/pipelines.html)
- [Jobs](https://docs.gitlab.com/ee/api/jobs.html)
- [Projects](https://docs.gitlab.com/ee/api/projects.html)
- [Project milestones](https://docs.gitlab.com/ee/api/milestones.html)
- [Project Merge Requests](https://docs.gitlab.com/ee/api/merge_requests.html)
- [Users](https://docs.gitlab.com/ee/api/users.html)
- [Groups](https://docs.gitlab.com/ee/api/group_milestones.html)
- [Group Milestones](https://docs.gitlab.com/ee/api/users.html)
- [Group and Project members](https://docs.gitlab.com/ee/api/members.html)
- [Tags](https://docs.gitlab.com/ee/api/tags.html)
- [Releases](https://docs.gitlab.com/ee/api/releases/index.html)
- [Group Labels](https://docs.gitlab.com/ee/api/group_labels.html)
- [Project Labels](https://docs.gitlab.com/ee/api/labels.html)
- [Epics](https://docs.gitlab.com/ee/api/epics.html) (only available for GitLab Ultimate and GitLab.com Gold accounts)
- [Epic Issues](https://docs.gitlab.com/ee/api/epic_issues.html) (only available for GitLab Ultimate and GitLab.com Gold accounts)
- [Vulnerabilities](https://docs.gitlab.com/ee/api/project_vulnerabilities.html)
- [Group Variables](https://docs.gitlab.com/ee/api/group_level_variables.html)
- [Project Variables](https://docs.gitlab.com/ee/api/project_level_variables.html)

See also:

- [Gitlab REST API Reference](https://docs.gitlab.com/ee/api/README.html)

## [Optional] Create an initial state file

You can provide JSON file that contains a date for the API endpoints
to force the application to only fetch data newer than those dates.
If you omit the file it will fetch all GitLab data

```json
{
  "project_278964": "2017-01-17T00:00:00Z",
  "project_278964_issues": "2017-01-17T00:00:00Z",
  "project_278964_merge_requests": "2017-01-17T00:00:00Z",
  "project_278964_commits": "2017-01-17T00:00:00Z"
}
```

Note:
- You have to provide the id of each project you are syncing. For example, in the case of `gitlab-org/gitlab` it is 278964.
- You can find the Project ID for a project in the homepage for the project, under its name.

To use your state file input, run `tap-gitlab` with:

```bash
tap-gitlab --config config.json [--state state.json]
```

### Source Authentication and Authorization

To get a GitLab access token:

- Login to your GitLab account.
- Navigate to your profile page.
- Create an access token with 'read_api' permissions.

## Usage

You can easily run `tap-gitlab` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-gitlab --version
tap-gitlab --help
tap-gitlab --config CONFIG --discover > ./catalog.json
```

## Developer Resources


### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_gitlab/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-gitlab` CLI interface directly using `poetry run`:

```bash
poetry run tap-gitlab --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-gitlab
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-gitlab --version
# OR run a test `elt` pipeline:
meltano elt tap-gitlab target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.
