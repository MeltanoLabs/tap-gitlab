"""Stream type classes for tap-gitlab."""

from typing import Any, Dict, List, Optional, cast

from tap_gitlab.client import GitLabStream, GroupBasedStream, ProjectBasedStream
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
        assert context is not None
        return {
            "project_id": record["id"],
            "project_path": context["project_path"],
        }


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

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Post process records."""
        result = super().post_process(row, context)
        if result is None:
            return None

        result["assignees"] = object_array_to_id_array(result["assignees"])
        return result


class ProjectMergeRequestsStream(ProjectBasedStream):
    """Gitlab Merge Requests stream."""

    name = "merge_requests"
    path = "/projects/{project_id}/merge_requests"
    primary_keys = ["id"]
    replication_key = "updated_at"
    parent_stream_type = ProjectsStream

    bookmark_param_name = "updated_after"
    extra_url_params = {"scope": "all"}

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Post process records."""
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

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Perform post processing, including queuing up any child stream types."""
        # Ensure child state record(s) are created
        assert context is not None
        return {
            "project_path": context["project_path"],
            "project_id": record["project_id"],
            "merge_request_iid": record["iid"],
        }


class MergeRequestCommitsStream(ProjectBasedStream):
    """Gitlab Commits stream."""

    name = "merge_request_commits"
    path = "/projects/{project_id}/merge_requests/{merge_request_iid}/commits"
    primary_keys = ["project_id", "merge_request_iid", "commit_id"]
    parent_stream_type = ProjectMergeRequestsStream

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

        assert context is not None

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
        # Ensure child state record(s) are created
        assert context is not None
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
    primary_keys = ["project_id", "key"]
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
