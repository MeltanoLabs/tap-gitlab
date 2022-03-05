"""Stream type classes for tap-gitlab."""

from typing import Any, Dict, Optional

from tap_gitlab.client import GitLabStream, ProjectBasedStream, GroupBasedStream


# Project-Specific Streams


class ProjectsStream(ProjectBasedStream):
    """Gitlab Projects stream."""

    name = "projects"
    path = "/projects/{project_id}"
    primary_keys = ["id"]
    replication_key = "last_activity_at"
    is_sorted = True
    extra_url_params = {"statistics": 1}


class IssuesStream(ProjectBasedStream):
    """Gitlab Issues stream."""

    name = "issues"
    path = "/projects/{project_id}/issues"
    primary_keys = ["id"]
    replication_key = "updated_at"
    bookmark_param_name = "updated_after"
    is_sorted = True
    extra_url_params = {"scope": "all"}


class CommitsStream(ProjectBasedStream):
    """Gitlab Commits stream."""

    name = "commits"
    path = "/projects/{project_id}/repository/commits"
    primary_keys = ["id"]
    replication_key = "created_at"
    is_sorted = False
    extra_url_params = {"with_stats": "true"}


class BranchesStream(ProjectBasedStream):
    name = "branches"
    path = "/projects/{project_id}/repository/branches"
    primary_keys = ["project_id", "name"]


class PipelinesStream(ProjectBasedStream):
    name = "pipelines"
    path = "/projects/{project_id}/pipelines"
    primary_keys = ["id"]
    replication_key = "updated_at"
    bookmark_param_name = "updated_after"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        context = super().get_child_context(record, context)
        context["pipeline_id"] = record["id"]
        return context


class PipelinesExtendedStream(ProjectBasedStream):
    name = "pipelines_extended"
    path = "/projects/{project_id}/pipelines/{pipeline_id}"
    primary_keys = ["id"]
    parent_stream_type = PipelinesStream


class PipelineJobsStream(ProjectBasedStream):
    name = "jobs"
    path = "/projects/{project_id}/pipelines/{pipeline_id}/jobs"
    primary_keys = ["id"]
    parent_stream_type = PipelinesStream  # Stream should wait for parents to complete.


class ProjectMilestonesStream(ProjectBasedStream):
    name = "project_milestones"
    path = "/projects/{project_id}/milestones"
    primary_keys = ["id"]
    schema_filename = "milestones.json"


class ProjectMergeRequestsStream(ProjectBasedStream):
    name = "merge_requests"
    path = "/projects/{project_id}/merge_requests"
    primary_keys = ["id"]
    replication_key = "updated_at"
    bookmark_param_name = "updated_after"
    extra_url_params = {"scope": "all"}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Perform post processing, including queuing up any child stream types."""
        # Ensure child state record(s) are created
        assert context is not None
        return {
            "project_id": context["project_id"],
            "merge_request_id": record["iid"],
        }


class MergeRequestCommitsStream(ProjectBasedStream):
    """Gitlab Commits stream."""

    name = "merge_request_commits"
    path = "/projects/{project_id}/merge_requests/{merge_request_id}/commits"
    primary_keys = ["project_id", "merge_request_iid", "commit_id"]
    parent_stream_type = ProjectMergeRequestsStream


class ProjectUsersStream(ProjectBasedStream):
    name = "users"
    path = "/projects/{project_id}/users"
    primary_keys = ["id"]


class ProjectMembersStream(ProjectBasedStream):
    name = "project_members"
    path = "/projects/{project_id}/members"
    primary_keys = ["project_id", "id"]


class ProjectLabelsStream(ProjectBasedStream):
    name = "project_labels"
    path = "/projects/{project_id}/labels"
    primary_keys = ["project_id", "id"]


class ProjectVulnerabilitiesStream(ProjectBasedStream):
    name = "vulnerabilities"
    path = "/projects/{project_id}/vulnerabilities"
    primary_keys = ["id"]


class ProjectVariablesStream(ProjectBasedStream):
    name = "project_variables"
    path = "/projects/{project_id}/variables"
    primary_keys = ["group_id", "key"]


# Group-Specific Streams


class GroupsStream(GroupBasedStream):
    name = "groups"
    path = "/groups/{group_id}"
    primary_keys = ["id"]


class GroupProjectsStream(GroupBasedStream):
    """Gitlab Projects stream."""

    name = "group_projects"
    path = "/groups/{group_id}/projects"
    primary_keys = ["id"]


class GroupMilestonesStream(GroupBasedStream):
    name = "group_milestones"
    path = "/groups/{group_id}/milestones"
    primary_keys = ["id"]
    schema_filename = "milestones.json"


class GroupMembersStream(GroupBasedStream):
    name = "group_members"
    path = "/groups/{group_id}/members"
    primary_keys = ["group_id", "id"]


class GroupLabelsStream(GroupBasedStream):
    name = "group_labels"
    path = "/groups/{group_id}/labels"
    primary_keys = ["group_id", "id"]


class GroupEpicsStream(GroupBasedStream):
    """Gitlab Epics stream."""

    name = "epics"
    path = "/groups/{group_id}/epics"
    primary_keys = ["id"]
    replication_key = "updated_at"
    bookmark_param_name = "updated_after"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Perform post processing, including queuing up any child stream types."""
        # Ensure child state record(s) are created
        return {
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
    name = "group_variables"
    path = "/groups/{group_id}/variables"
    primary_keys = ["project_id", "key"]


# Global streams


class GlobalSiteUsersStream(GitLabStream):
    name = "site_users"
    path = "/users"
    primary_keys = ["id"]
    schema_filename = "users.json"


# TODO: Failing with:
# FatalAPIError: 400 Client Error: Bad Request for path:
# /projects/{project_id}/releases
# class ReleasesStream(ProjectBasedStream):
#     """Gitlab Releases stream."""

#     name = "releases"
#     path = "/projects/{project_id}/releases"
#     primary_keys = ["project_id", "commit_id", "tag_name"]
#     replication_key = None


# TODO: Failing with:
# FatalAPIError: 400 Client Error: Bad Request for path: /projects/{project_id}/repository/tags
# class TagsStream(ProjectBasedStream):
#     name = "tags"
#     path = "/projects/{project_id}/repository/tags"
#     primary_keys = ["project_id", "commit_id", "name"]
