"""Stream type classes for tap-gitlab."""

from typing import Any, Dict, Iterable, List, Optional, cast

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_gitlab.client import (
    GitLabStream,
    GroupBasedStream,
    NoSinceProjectBasedStream,
    ProjectBasedStream,
)
from tap_gitlab.transforms import pop_nested_id

# Project-Specific Streams

# all user objects in non-user streams follow the same structure
# keep it here to help readability in schema definitions below
user_object = th.ObjectType(
    th.Property("id", th.IntegerType),
    th.Property("username", th.StringType),
    th.Property("name", th.StringType),
    th.Property("state", th.StringType),
    th.Property("avatar_url", th.StringType),
    th.Property("web_url", th.StringType),
)


class ProjectsStream(ProjectBasedStream):
    """Gitlab Projects stream."""

    name = "projects"
    path = "/projects/{project_path}"
    primary_keys = ["id"]
    replication_key = "last_activity_at"
    is_sorted = False
    extra_url_params = {"statistics": 1}
    schema_filepath = None  # to allow the use of schema below
    state_partitioning_keys = ["id"]

    @property
    def partitions(self) -> List[dict]:
        """Return a list of partition key dicts (if applicable), otherwise None."""
        if "{project_path}" in self.path:
            if "projects" not in self.config:
                raise ValueError(
                    f"Missing `projects` setting which is required for the "
                    f"'{self.name}' stream."
                )

        return [
            {"project_path": id}
            for id in cast(list, self.config["projects"].split(" "))
        ]

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Post process records."""
        result = super().post_process(row, context)
        if result is None:
            return None

        if "last_activity_at" not in result:
            raise ValueError(
                f"Missing 'last_activity_at' field for project '{self.path}'."
            )
        result["owner_id"] = pop_nested_id(result, "owner")
        return result

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Perform post processing, including queuing up any child stream types."""
        assert context is not None  # Tell linter that context is non-null
        return {
            "project_id": record["id"],
            "project_path": context["project_path"],
        }

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(context, next_page_token)
        # include license info for the project
        params["license"] = True
        return params

    schema = th.PropertiesList(  # type: ignore
        th.Property("id", th.IntegerType),
        th.Property("description", th.StringType),
        th.Property("name", th.StringType),
        th.Property("name_with_namespace", th.StringType),
        th.Property("path", th.StringType),
        th.Property("path_with_namespace", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("default_branch", th.StringType),
        # tag_list deprecated in favour of topics
        # https://docs.gitlab.com/ee/api/projects.html
        th.Property("tag_list", th.ArrayType(th.StringType)),
        th.Property("topics", th.ArrayType(th.StringType)),
        th.Property("ssh_url_to_repo", th.StringType),
        th.Property("http_url_to_repo", th.StringType),
        th.Property("web_url", th.StringType),
        th.Property("readme_url", th.StringType),
        th.Property("avatar_url", th.StringType),
        th.Property("forks_count", th.IntegerType),
        th.Property("star_count", th.IntegerType),
        th.Property("last_activity_at", th.DateTimeType),
        # gitlab handles project owners differently depending on their types:
        # "users" have both a namespace and owner object (note `id` values do not match)
        # "groups" only have a namespace filled, and owner is empty. `namespace__id`
        # can be passed to `/groups/:id` and return the expected group,
        # but not to `/users/:id`.
        th.Property(
            "namespace",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
                th.Property("path", th.StringType),
                th.Property("kind", th.StringType),
                th.Property("full_path", th.StringType),
                th.Property("parent_id", th.IntegerType),
            ),
        ),
        th.Property("owner", user_object),
        th.Property("archived", th.BooleanType),
        th.Property("visibility", th.StringType),
        th.Property("visibility_level", th.IntegerType),
        th.Property("open_issues_count", th.IntegerType),
        th.Property("creator_id", th.IntegerType),
        th.Property("public", th.BooleanType),
        th.Property("public_builds", th.BooleanType),
        th.Property("only_allow_merge_if_all_discussions_are_resolved", th.BooleanType),
        th.Property("only_allow_merge_if_build_succeeds", th.BooleanType),
        th.Property("request_access_enabled", th.BooleanType),
        th.Property("issues_enabled", th.BooleanType),
        th.Property("shared_runners_enabled", th.BooleanType),
        th.Property("snippets_enabled", th.BooleanType),
        th.Property("wiki_enabled", th.BooleanType),
        th.Property("license_url", th.StringType),
        th.Property(
            "license",
            th.ObjectType(
                th.Property("key", th.StringType),
                th.Property("name", th.StringType),
                th.Property("nickname", th.StringType),
                th.Property("html_url", th.StringType),
                th.Property("source_url", th.StringType),
            ),
        ),
    ).to_dict()


class LanguagesStream(ProjectBasedStream):
    """Gitlab Languages stream for a project."""

    # docs: https://docs.gitlab.com/ee/api/projects.html#languages
    name = "languages"
    path = "/projects/{project_id}/languages"
    primary_keys = ["project_id", "language_name"]
    parent_stream_type = ProjectsStream
    schema_filepath = None  # to allow the use of schema below

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Return an iterator of {language_name, percent}.

        Parse the language response and reformat to return as an
        iterator of [{language_name: "Python", percent: 23.45}].
        """
        languages_json = response.json()
        for key, value in languages_json.items():
            yield {"language_name": key, "percent": value}

    schema = th.PropertiesList(  # type: ignore
        th.Property("project_id", th.IntegerType),
        th.Property("language_name", th.StringType),
        th.Property("percent", th.NumberType),
    ).to_dict()


class IssuesStream(ProjectBasedStream):
    """Gitlab Issues stream."""

    name = "issues"
    path = "/projects/{project_id}/issues"
    primary_keys = ["id"]
    replication_key = "updated_at"
    is_sorted = True
    parent_stream_type = ProjectsStream

    bookmark_param_name = "updated_after"
    extra_url_params = {"scope": "all"}
    schema_filepath = None  # to allow the use of schema below

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Post process records."""
        result = super().post_process(row, context)
        if result is None:
            return None

        # XXX: breaks backwards compatibility
        # result["assignees"] = object_array_to_id_array(result["assignees"])
        return result

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return the context for child streams such as issue_notes."""
        assert context is not None
        context["issue_iid"] = record["iid"]
        return context

    schema = th.PropertiesList(  # type: ignore
        th.Property("id", th.IntegerType),
        th.Property("iid", th.IntegerType),
        th.Property("project_id", th.IntegerType),
        th.Property("milestone_id", th.IntegerType),
        th.Property("epic_id", th.IntegerType),
        th.Property("author", user_object),
        th.Property("assignees", th.ArrayType(user_object)),
        # XXX: breaks backwards compatibility
        th.Property("closed_by", user_object),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
        th.Property("state", th.StringType),
        th.Property("labels", th.ArrayType(th.StringType)),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("closed_at", th.DateTimeType),
        th.Property("subscribed", th.BooleanType),
        th.Property("upvotes", th.IntegerType),
        th.Property("downvotes", th.IntegerType),
        th.Property("merge_requests_count", th.IntegerType),
        th.Property("user_notes_count", th.IntegerType),
        th.Property("due_date", th.StringType),
        th.Property("weight", th.IntegerType),
        th.Property("web_url", th.StringType),
        th.Property("confidential", th.BooleanType),
        th.Property("discussion_locked", th.BooleanType),
        th.Property("has_tasks", th.BooleanType),
        th.Property("task_status", th.StringType),
        th.Property("time_estimate", th.IntegerType),
        th.Property("total_time_spent", th.IntegerType),
        th.Property("human_time_estimate", th.StringType),
        th.Property("human_total_time_spent", th.StringType),
    ).to_dict()


class NoteableStream(NoSinceProjectBasedStream):
    """Abstract class for gitlab's Noteable API stream."""

    # docs: https://docs.gitlab.com/ee/api/notes.html#list-project-issue-notes

    primary_keys = ["id"]
    schema_filepath = None
    # set this to project_id only to avoid saving a bookmark for each issue
    # which would result in the state object becoming unusably large
    state_partitioning_keys = ["project_id"]
    extra_url_params = {"sort": "desc", "order_by": "updated_at"}
    replication_key = "updated_at"
    ignore_parent_replication_key = False

    schema = th.PropertiesList(  # type: ignore
        th.Property("id", th.IntegerType),
        th.Property("project_id", th.IntegerType),
        th.Property("noteable_id", th.IntegerType),
        th.Property("noteable_iid", th.IntegerType),
        th.Property("noteable_type", th.StringType),
        th.Property("body", th.StringType),
        th.Property("attachment", th.StringType),
        th.Property("author", user_object),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("confidential", th.BooleanType),
    ).to_dict()


class IssueNotesStream(NoteableStream):
    """Gitlab Issues Notes (comments) Stream."""

    name = "issue_notes"
    parent_stream_type = IssuesStream
    path = "/projects/{project_path}/issues/{issue_iid}/notes"


class ProjectMergeRequestsStream(ProjectBasedStream):
    """Gitlab Merge Requests stream."""

    name = "merge_requests"
    path = "/projects/{project_id}/merge_requests"
    primary_keys = ["id"]
    replication_key = "updated_at"
    parent_stream_type = ProjectsStream

    bookmark_param_name = "updated_after"
    extra_url_params = {"scope": "all"}
    schema_filepath = None  # to allow the use of schema below

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Post process records."""
        result = super().post_process(row, context)
        if result is None:
            return None

        time_stats = result.pop("time_stats", {})
        for time_key in [
            "time_estimate",
            "total_time_spent",
            "human_time_estimate",
            "human_total_time_spent",
        ]:
            result[time_key] = time_stats.get(time_key, None)

        return result

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Perform post processing, including queuing up any child stream types."""
        # Ensure child state record(s) are created
        assert context is not None  # Tell linter that context is non-null
        return {
            "project_path": context["project_path"],
            "project_id": record["project_id"],
            "merge_request_iid": record["iid"],
        }

    schema = th.PropertiesList(  # type: ignore
        th.Property("id", th.IntegerType),
        th.Property("iid", th.IntegerType),
        th.Property("project_id", th.IntegerType),
        th.Property("milestone_id", th.IntegerType),
        th.Property("epic_id", th.IntegerType),
        th.Property("author", user_object),
        th.Property("assignees", th.ArrayType(user_object)),
        th.Property("reviewers", th.ArrayType(user_object)),
        # merged_by is deprecated
        th.Property("merge_user", user_object),
        th.Property("closed_by", user_object),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
        th.Property("state", th.StringType),
        th.Property("labels", th.ArrayType(th.StringType)),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("merged_at", th.DateTimeType),
        th.Property("closed_at", th.DateTimeType),
        th.Property("target_project_id", th.IntegerType),
        th.Property("target_branch", th.StringType),
        th.Property("source_project_id", th.IntegerType),
        th.Property("source_branch", th.StringType),
        th.Property("subscribed", th.BooleanType),
        th.Property("draft", th.BooleanType),
        th.Property("work_in_progress", th.BooleanType),
        th.Property("merge_when_pipeline_succeeds", th.BooleanType),
        th.Property("merge_status", th.StringType),
        th.Property("has_conflicts", th.BooleanType),
        th.Property("upvotes", th.IntegerType),
        th.Property("downvotes", th.IntegerType),
        th.Property("sha", th.StringType),
        th.Property("squash", th.BooleanType),
        th.Property("squash_commit_sha", th.StringType),
        th.Property("user_notes_count", th.IntegerType),
        th.Property("should_remove_source_branch", th.BooleanType),
        th.Property("force_remove_source_branch", th.BooleanType),
        th.Property("allow_collaboration", th.BooleanType),
        th.Property("allow_maintainer_to_push", th.BooleanType),
        th.Property("due_date", th.StringType),
        th.Property("weight", th.IntegerType),
        th.Property("web_url", th.StringType),
        th.Property("confidential", th.BooleanType),
        th.Property("discussion_locked", th.BooleanType),
        th.Property("has_tasks", th.BooleanType),
        th.Property("task_status", th.StringType),
        th.Property("time_estimate", th.IntegerType),
        th.Property("total_time_spent", th.IntegerType),
        th.Property("human_time_estimate", th.StringType),
        th.Property("human_total_time_spent", th.StringType),
    ).to_dict()


class MergeRequestNotesStream(NoteableStream):
    """Gitlab Merge Request Notes (comments) Stream."""

    # docs: https://docs.gitlab.com/ee/api/notes.html#list-all-merge-request-notes

    name = "merge_request_notes"
    parent_stream_type = ProjectMergeRequestsStream
    path = "/projects/{project_path}/merge_requests/{merge_request_iid}/notes"


class MergeRequestCommitsStream(ProjectBasedStream):
    """Gitlab Commits stream."""

    name = "merge_request_commits"
    path = "/projects/{project_id}/merge_requests/{merge_request_iid}/commits"
    primary_keys = ["project_id", "merge_request_iid", "commit_id"]
    parent_stream_type = ProjectMergeRequestsStream
    replication_key = "created_at"
    is_sorted = False
    extra_url_params = {"with_stats": "true"}
    schema_filepath = None  # to allow the use of schema below

    schema = th.PropertiesList(  # type: ignore
        th.Property("project_id", th.IntegerType),
        th.Property("merge_request_iid", th.IntegerType),
        th.Property("commit_id", th.StringType),
        th.Property("commit_short_id", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("parent_ids", th.ArrayType(th.StringType())),
        th.Property("title", th.StringType),
        th.Property("message", th.StringType),
        th.Property("author_name", th.StringType),
        th.Property("author_email", th.StringType),
        th.Property("authored_date", th.DateTimeType),
        th.Property("committer_name", th.StringType),
        th.Property("committer_email", th.StringType),
        th.Property("committed_date", th.DateTimeType),
        th.Property("web_url", th.StringType),
        th.Property(
            "stats",
            th.ObjectType(
                th.Property("additions", th.IntegerType),
                th.Property("deletions", th.IntegerType),
                th.Property("total", th.IntegerType),
            ),
        ),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Post process records."""
        result = super().post_process(row, context)
        if result is None:
            return None

        for orig, renamed in {"id": "commit_id", "short_id": "commit_short_id"}.items():
            try:
                result[renamed] = result.pop(orig)
            except KeyError as ex:
                raise KeyError(f"Missing property '{orig}' in record: {result}") from ex

        return result


class CommitsStream(ProjectBasedStream):
    """Gitlab Commits stream."""

    name = "commits"
    path = "/projects/{project_id}/repository/commits"
    primary_keys = ["id"]
    replication_key = "created_at"
    is_sorted = False
    parent_stream_type = ProjectsStream
    extra_url_params = {"with_stats": "true"}
    schema_filepath = None  # to allow the use of schema below

    schema = th.PropertiesList(  # type: ignore
        th.Property("project_id", th.IntegerType),
        th.Property("id", th.StringType),
        th.Property("short_id", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("parent_ids", th.ArrayType(th.StringType())),
        th.Property("title", th.StringType),
        th.Property("message", th.StringType),
        th.Property("author_name", th.StringType),
        th.Property("author_email", th.StringType),
        th.Property("authored_date", th.DateTimeType),
        th.Property("committer_name", th.StringType),
        th.Property("committer_email", th.StringType),
        th.Property("committed_date", th.DateTimeType),
        th.Property("web_url", th.StringType),
        th.Property(
            "stats",
            th.ObjectType(
                th.Property("additions", th.IntegerType),
                th.Property("deletions", th.IntegerType),
                th.Property("total", th.IntegerType),
            ),
        ),
    ).to_dict()


class BranchesStream(ProjectBasedStream):
    """Gitlab Branches stream."""

    name = "branches"
    path = "/projects/{project_id}/repository/branches"
    primary_keys = ["project_id", "name"]
    parent_stream_type = ProjectsStream

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Post process records."""
        result = super().post_process(row, context)
        if result is None:
            return None

        assert context is not None  # Tell linter that context is non-null

        result["project_id"] = context["project_id"]
        result["commit_id"] = pop_nested_id(result, "commit")
        return result


class PipelinesStream(ProjectBasedStream):
    """Gitlab Pipelines stream."""

    name = "pipelines"
    path = "/projects/{project_id}/pipelines"
    primary_keys = ["id"]
    replication_key = "updated_at"
    parent_stream_type = ProjectsStream

    bookmark_param_name = "updated_after"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Perform post processing, including queuing up any child stream types."""
        context = super().get_child_context(record, context)
        context["pipeline_id"] = record["id"]
        return context


class PipelinesExtendedStream(ProjectBasedStream):
    """Gitlab extended Pipelines stream."""

    name = "pipelines_extended"
    path = "/projects/{project_id}/pipelines/{pipeline_id}"
    primary_keys = ["id"]
    parent_stream_type = PipelinesStream


class PipelineJobsStream(ProjectBasedStream):
    """Gitlab Pipeline Jobs stream."""

    name = "jobs"
    path = "/projects/{project_id}/pipelines/{pipeline_id}/jobs"
    primary_keys = ["id"]
    parent_stream_type = PipelinesStream  # Stream should wait for parents to complete.


class ProjectMilestonesStream(ProjectBasedStream):
    """Gitlab Project Milestones stream."""

    name = "project_milestones"
    path = "/projects/{project_id}/milestones"
    primary_keys = ["id"]
    schema_filename = "milestones.json"
    parent_stream_type = ProjectsStream


class ProjectUsersStream(ProjectBasedStream):
    """Gitlab Project Users stream."""

    name = "users"
    path = "/projects/{project_id}/users"
    primary_keys = ["id"]
    parent_stream_type = ProjectsStream


class ProjectMembersStream(ProjectBasedStream):
    """Gitlab Project Members stream."""

    name = "project_members"
    path = "/projects/{project_id}/members"
    primary_keys = ["project_id", "id"]
    parent_stream_type = ProjectsStream


class ProjectLabelsStream(ProjectBasedStream):
    """Gitlab Project Labels stream."""

    name = "project_labels"
    path = "/projects/{project_id}/labels"
    primary_keys = ["project_id", "id"]
    parent_stream_type = ProjectsStream
    schema_filepath = None
    state_partitioning_keys = ["project_path"]
    extra_url_params = {"per_page": 100}

    schema = th.PropertiesList(  # type: ignore
        th.Property("id", th.IntegerType),
        th.Property("project_id", th.IntegerType),
        th.Property("project_path", th.StringType),
        th.Property("name", th.StringType),
        th.Property("color", th.StringType),
        th.Property("text_color", th.StringType),
        th.Property("description", th.StringType),
        th.Property("open_issues_count", th.IntegerType),
        th.Property("closed_issues_count", th.IntegerType),
        th.Property("open_merge_requests_count", th.IntegerType),
        th.Property("subscribed", th.BooleanType),
        th.Property("priority", th.IntegerType),
        th.Property("is_project_label", th.BooleanType),
    ).to_dict()


class ProjectVulnerabilitiesStream(ProjectBasedStream):
    """Project Vulnerabilities stream."""

    name = "vulnerabilities"
    path = "/projects/{project_id}/vulnerabilities"
    primary_keys = ["id"]
    parent_stream_type = ProjectsStream


class ProjectVariablesStream(ProjectBasedStream):
    """Project Variables stream."""

    name = "project_variables"
    path = "/projects/{project_id}/variables"
    primary_keys = ["project_id", "key"]
    parent_stream_type = ProjectsStream


# Group-Specific Streams


class GroupsStream(GroupBasedStream):
    """Gitlab Groups stream."""

    name = "groups"
    path = "/groups/{group_path}"
    primary_keys = ["id"]

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Perform post processing, including queuing up any child stream types."""
        assert context is not None  # Tell linter that context is non-null
        return {
            "group_path": context["group_path"],
            "group_id": record["id"],
        }


class GroupProjectsStream(GroupBasedStream):
    """Gitlab Projects stream."""

    name = "group_projects"
    path = "/groups/{group_path}/projects"
    primary_keys = ["id"]
    parent_stream_type = GroupsStream


class GroupMilestonesStream(GroupBasedStream):
    """Gitlab Group Milestones stream."""

    name = "group_milestones"
    path = "/groups/{group_path}/milestones"
    primary_keys = ["id"]
    schema_filename = "milestones.json"
    parent_stream_type = GroupsStream


class GroupMembersStream(GroupBasedStream):
    """Gitlab Group Members stream."""

    name = "group_members"
    path = "/groups/{group_path}/members"
    primary_keys = ["group_id", "id"]
    parent_stream_type = GroupsStream


class GroupLabelsStream(GroupBasedStream):
    """Gitlab Group Labels stream."""

    name = "group_labels"
    path = "/groups/{group_path}/labels"
    primary_keys = ["group_id", "id"]
    parent_stream_type = GroupsStream


class GroupEpicsStream(GroupBasedStream):
    """Gitlab Epics stream."""

    name = "epics"
    path = "/groups/{group_path}/epics"
    primary_keys = ["id"]
    replication_key = "updated_at"
    bookmark_param_name = "updated_after"
    parent_stream_type = GroupsStream

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Perform post processing, including queuing up any child stream types."""
        # Ensure child state record(s) are created
        return {
            "group_path": record["group_path"],
            "group_id": record["group_id"],
            "epic_id": record["id"],
            "epic_iid": record["iid"],
        }


class GroupEpicIssuesStream(GroupBasedStream):
    """EpicIssues stream class."""

    name = "epic_issues"
    path = "/groups/{group_id}/epics/{epic_iid}/issues"
    primary_keys = ["group_id", "epic_iid", "epic_issue_id"]
    parent_stream_type = GroupEpicsStream  # Stream should wait for parents to complete.

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in parameterization."""
        result = super().get_url_params(context, next_page_token)
        if not context or "epic_id" not in context:
            raise ValueError("Cannot sync epic issues without already known epic IDs.")
        return result


class GroupVariablesStream(GroupBasedStream):
    """Gitlab Group Variables stream."""

    name = "group_variables"
    path = "/groups/{group_id}/variables"
    primary_keys = ["group_id", "key"]
    parent_stream_type = GroupsStream


# Global streams


class GlobalSiteUsersStream(GitLabStream):
    """Gitlab Global Site Users stream."""

    name = "site_users"
    path = "/users"
    primary_keys = ["id"]
    schema_filename = "users.json"


class TagsStream(ProjectBasedStream):
    """Gitlab Tags stream."""

    name = "tags"
    path = "/projects/{project_path}/repository/tags"
    primary_keys = ["project_id", "commit_id", "name"]
    parent_stream_type = ProjectsStream

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Post process records."""
        result = super().post_process(row, context)
        if result is None:
            return None

        result["commit_id"] = result.pop("commit")["id"]
        return result


class ReleasesStream(ProjectBasedStream):
    """Gitlab Releases stream."""

    name = "releases"
    path = "/projects/{project_path}/releases"
    primary_keys = ["project_id", "commit_id", "tag_name"]
    replication_key = None
    parent_stream_type = ProjectsStream

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Post process records."""
        result = super().post_process(row, context)
        if result is None:
            return None

        result["commit_id"] = result.pop("commit")["id"]
        return result
