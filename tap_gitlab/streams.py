"""Stream type classes for tap-gitlab."""

from typing import Any, Dict, Optional

from tap_gitlab.client import GitLabStream, ProjectBasedStream, GroupBasedStream
from tap_gitlab.transforms import object_array_to_id_array, pop_nested_id

# Project-Specific Streams


class ProjectsStream(ProjectBasedStream):
    """Gitlab Projects stream."""

    name = "projects"
    path = "/projects/{project_path}"
    primary_keys = ["id"]
    replication_key = "last_activity_at"
    is_sorted = True
    extra_url_params = {"statistics": 1}

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        result = super().post_process(row, context)
        if result is None:
            return None

        result["owner_id"] = pop_nested_id(result, "owner")
        return result

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        assert context is not None
        return {
            "project_id": record["id"],
            "project_path": context["project_path"],
        }


class IssuesStream(ProjectBasedStream):
    """Gitlab Issues stream."""

    name = "issues"
    path = "/projects/{project_path}/issues"
    primary_keys = ["id"]
    replication_key = "updated_at"
    bookmark_param_name = "updated_after"
    is_sorted = True
    extra_url_params = {"scope": "all"}

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        result = super().post_process(row, context)
        if result is None:
            return None

        result["assignees"] = object_array_to_id_array(result["assignees"])
        return result


class ProjectMergeRequestsStream(ProjectBasedStream):
    name = "merge_requests"
    path = "/projects/{project_path}/merge_requests"
    primary_keys = ["id"]
    replication_key = "updated_at"
    bookmark_param_name = "updated_after"
    extra_url_params = {"scope": "all"}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Perform post processing, including queuing up any child stream types."""
        # Ensure child state record(s) are created
        assert context is not None
        return {
            "project_path": context["project_path"],
            "project_id": record["project_id"],
            "merge_request_id": record["iid"],
        }

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        result = super().post_process(row, context)
        if result is None:
            return None

        for key in ["author", "assignee", "milestone", "merge_by", "closed_by"]:
            result[f"{key}_id"] = pop_nested_id(result, key)
        result["assignees"] = object_array_to_id_array(result["assignees"])
        result["reviewers"] = object_array_to_id_array(result["reviewers"])

        time_stats = result.pop("time_stats", {})
        for time_key in [
            "time_estimate",
            "total_time_spent",
            "human_time_estimate",
            "human_total_time_spent",
        ]:
            result[time_key] = time_stats.get(time_key, None)

        return result


class CommitsStream(ProjectBasedStream):
    """Gitlab Commits stream."""

    name = "commits"
    path = "/projects/{project_path}/repository/commits"
    primary_keys = ["id"]
    replication_key = "created_at"
    is_sorted = False
    extra_url_params = {"with_stats": "true"}


class BranchesStream(ProjectBasedStream):
    name = "branches"
    path = "/projects/{project_path}/repository/branches"
    primary_keys = ["project_id", "name"]
    # TODO: Research why this fails:
    # parent_stream_type = ProjectsStream

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        result = super().post_process(row, context)
        if result is None:
            return None

        assert context is not None

        # TODO: Uncomment when parent relationship works
        # result["project_id"] = context["project_id"]
        result["commit_id"] = pop_nested_id(result, "commit")
        return result


class PipelinesStream(ProjectBasedStream):
    name = "pipelines"
    path = "/projects/{project_path}/pipelines"
    primary_keys = ["id"]
    replication_key = "updated_at"
    bookmark_param_name = "updated_after"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        context = super().get_child_context(record, context)
        context["pipeline_id"] = record["id"]
        return context


class PipelinesExtendedStream(ProjectBasedStream):
    name = "pipelines_extended"
    path = "/projects/{project_path}/pipelines/{pipeline_id}"
    primary_keys = ["id"]
    parent_stream_type = PipelinesStream


class PipelineJobsStream(ProjectBasedStream):
    name = "jobs"
    path = "/projects/{project_path}/pipelines/{pipeline_id}/jobs"
    primary_keys = ["id"]
    parent_stream_type = PipelinesStream  # Stream should wait for parents to complete.


class ProjectMilestonesStream(ProjectBasedStream):
    name = "project_milestones"
    path = "/projects/{project_path}/milestones"
    primary_keys = ["id"]
    schema_filename = "milestones.json"


class MergeRequestCommitsStream(ProjectBasedStream):
    """Gitlab Commits stream."""

    name = "merge_request_commits"
    path = "/projects/{project_path}/merge_requests/{merge_request_id}/commits"
    primary_keys = ["project_id", "merge_request_iid", "commit_id"]
    parent_stream_type = ProjectMergeRequestsStream


class ProjectUsersStream(ProjectBasedStream):
    name = "users"
    path = "/projects/{project_path}/users"
    primary_keys = ["id"]


class ProjectMembersStream(ProjectBasedStream):
    name = "project_members"
    path = "/projects/{project_path}/members"
    primary_keys = ["project_id", "id"]


class ProjectLabelsStream(ProjectBasedStream):
    name = "project_labels"
    path = "/projects/{project_path}/labels"
    primary_keys = ["project_id", "id"]


class ProjectVulnerabilitiesStream(ProjectBasedStream):
    name = "vulnerabilities"
    path = "/projects/{project_path}/vulnerabilities"
    primary_keys = ["id"]


class ProjectVariablesStream(ProjectBasedStream):
    name = "project_variables"
    path = "/projects/{project_path}/variables"
    primary_keys = ["group_id", "key"]


# Group-Specific Streams


class GroupsStream(GroupBasedStream):
    name = "groups"
    path = "/groups/{group_path}"
    primary_keys = ["id"]


class GroupProjectsStream(GroupBasedStream):
    """Gitlab Projects stream."""

    name = "group_projects"
    path = "/groups/{group_path}/projects"
    primary_keys = ["id"]


class GroupMilestonesStream(GroupBasedStream):
    name = "group_milestones"
    path = "/groups/{group_path}/milestones"
    primary_keys = ["id"]
    schema_filename = "milestones.json"


class GroupMembersStream(GroupBasedStream):
    name = "group_members"
    path = "/groups/{group_path}/members"
    primary_keys = ["group_id", "id"]


class GroupLabelsStream(GroupBasedStream):
    name = "group_labels"
    path = "/groups/{group_path}/labels"
    primary_keys = ["group_id", "id"]


class GroupEpicsStream(GroupBasedStream):
    """Gitlab Epics stream."""

    name = "epics"
    path = "/groups/{group_path}/epics"
    primary_keys = ["id"]
    replication_key = "updated_at"
    bookmark_param_name = "updated_after"

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
    path = "/groups/{group_path}/epics/{epic_iid}/issues"
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
    name = "group_variables"
    path = "/groups/{group_path}/variables"
    primary_keys = ["project_id", "key"]


# Global streams


class GlobalSiteUsersStream(GitLabStream):
    name = "site_users"
    path = "/users"
    primary_keys = ["id"]
    schema_filename = "users.json"


# TODO: Failing with:
# FatalAPIError: 400 Client Error: Bad Request for path:
# /projects/{project_path}/releases
# class ReleasesStream(ProjectBasedStream):
#     """Gitlab Releases stream."""

#     name = "releases"
#     path = "/projects/{project_path}/releases"
#     primary_keys = ["project_id", "commit_id", "tag_name"]
#     replication_key = None


# TODO: Failing with:
# FatalAPIError: 400 Client Error: Bad Request for path: /projects/{project_path}/repository/tags
# class TagsStream(ProjectBasedStream):
#     name = "tags"
#     path = "/projects/{project_path}/repository/tags"
#     primary_keys = ["project_id", "commit_id", "name"]
