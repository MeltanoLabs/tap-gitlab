# tap-gitlab

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

It is based on v0.5.1 of <https://github.com/singer-io/tap-gitlab>, but contains [many additional improvements](./CHANGELOG.md).

This tap:
- Pulls raw data from GitLab's [REST API](https://docs.gitlab.com/ee/api/README.html)
- Extracts the following resources from GitLab:
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
- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Quick start

1. Install

Currently this project is not hosted on Python Package Index. To install, run:
```
pip install git+https://gitlab.com/meltano/tap-gitlab.git
```

2. Get your GitLab access token

    - Login to your GitLab account
    - Navigate to your profile page
    - Create an access token

3. Create the config file

    Create a JSON file called `config.json` containing:
    - Access token you just created
    - API URL for your GitLab account. If you are using the public gitlab.com this will be `https://gitlab.com/api/v4`
    - Groups to track (space separated)    
    - Projects to track (space separated)

    Notes on group and project options:
    - either groups or projects need to be provided
    - filling in 'groups' but leaving 'projects' empty will sync all group projects.
    - filling in 'projects' but leaving 'groups' empty will sync selected projects.
    - filling in 'groups' and 'projects' will sync selected projects of those groups.

    ```json
    {
      "api_url": "https://gitlab.com",
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

    The `api_url` requires only the base URL of the GitLab instance, e.g. `https://gitlab.com`. `tap-gitlab` automatically uses the latest (v4) version of GitLab's API. If you really want to set a different API version, you can set the full API URL, e.g. `https://gitlab.com/api/v3`, but be warned that this tap is built for API v4.

    If `ultimate_license` is true (defaults to false), then the GitLab account used has access to the GitLab Ultimate or GitLab.com Gold features. It will enable fetching Epics, Epic Issues and other entities available for GitLab Ultimate and GitLab.com Gold accounts.

    If `fetch_merge_request_commits` is true (defaults to false), then for each Merge Request, also fetch the MR's commits and create the join table `merge_request_commits` with the Merge Request and related Commit IDs. In the current version of GitLab's API, this operation requires one API call per Merge Request, so setting this to True can slow down considerably the end-to-end extraction time. For example, in a project like `gitlab-org/gitlab-foss`, this would result to 15x more API calls than required for fetching all the other Entities supported by `tap-gitlab`.

    If `fetch_pipelines_extended` is true (defaults to false), then for every Pipeline fetched with `sync_pipelines` (which returns N pages containing all pipelines per project), also fetch extended details of each of these pipelines with `sync_pipelines_extended`. Similar concerns as those related to `fetch_merge_request_commits` apply here - every pipeline fetched with `sync_pipelines_extended` requires a separate API call.

    If `fetch_group_variables` is true (defaults to false), then Group-level CI/CD variables will be retrieved for each available / specified group. This feature is treated as an opt-in to prevent users from accidentally extracting any potential secrets stored as Group-level CI/CD variables.

    If `fetch_project_variables` is true (defaults to false), then Project-level CI/CD variables will be retrieved for each available / specified project. This feature is treated as an opt-in to prevent users from accidentally extracting any potential secrets stored as Project-level CI/CD variables.

4. [Optional] Create the initial state file

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

5. Run the application

    `tap-gitlab` can be run with:

    ```bash
    tap-gitlab --config config.json [--state state.json]
    ```

---

Copyright &copy; 2018 Stitch
