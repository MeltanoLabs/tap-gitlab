"""GitLab tap class."""

import inspect
from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_gitlab import streams
from tap_gitlab.caching import setup_requests_cache
from tap_gitlab.client import GroupBasedStream
from tap_gitlab.streams import GitLabStream, ProjectBasedStream

OPTIN_STREAM_NAMES = [
    "merge_request_commits",
    "pipelines_extended",
    "group_variables",
    "project_variables",
    "site_users",
]
ULTIMATE_LICENSE_STREAM_NAMES = ["epics", "epic_issues"]


class TapGitLab(Tap):
    """GitLab tap class."""

    name = "tap-gitlab"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            required=False,
            description=(
                "Optionally overrides the default base URL for the Gitlab API. "
                "If no path is provided, the base URL will be appended with `/api/v4`. "
                "E.g. 'https://gitlab.com' becomes 'https://gitlab.com/api/v4'."
            ),
        ),
        th.Property(
            "private_token",
            th.StringType,
            required=True,
            description="An access token to use when calling to the Gitlab API.",
        ),
        th.Property(
            "groups",
            th.StringType,
            required=False,
            description=(
                "A space delimited list of group ids, e.g. "
                "'orgname1 orgname2 orgname3'"
            ),
        ),
        th.Property(
            "projects",
            th.StringType,
            required=False,
            description=(
                "A space delimited list of project ids, e.g. "
                "'orgname/projectname1 orgname/projectname2"
            ),
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            description=(
                "Optional. If provided, this is the furthest date for which "
                "data will be retrieved."
            ),
        ),
        th.Property(
            "ultimate_license",
            th.BooleanType,
            required=False,
            description=(
                "If not set to 'true', the following streams will be ignored: "
                "'epics' and 'epic_issues'."
            ),
            default=False,
        ),
        th.Property(
            "fetch_merge_request_commits",
            th.BooleanType,
            required=False,
            description=(
                "If not set to 'true', the 'merge_request_commits' stream will be "
                "ignored."
            ),
            default=False,
        ),
        th.Property(
            "fetch_pipelines_extended",
            th.BooleanType,
            required=False,
            description=(
                "If not set to 'true', the 'pipelines_extended' stream will be ignored."
            ),
            default=False,
        ),
        th.Property(
            "fetch_group_variables",
            th.BooleanType,
            required=False,
            description=(
                "If not set to 'true', the 'group_variables' stream will be ignored."
            ),
            default=False,
        ),
        th.Property(
            "fetch_project_variables",
            th.BooleanType,
            required=False,
            description=(
                "If not set to 'true', the 'project_variables' stream will be ignored."
            ),
            default=False,
        ),
        th.Property(
            "fetch_site_users",
            th.BooleanType,
            required=False,
            description=(
                "Unless set to 'false', the global 'site_users' stream will be "
                "included."
            ),
            default=True,
        ),
        th.Property(
            "requests_cache_path",
            th.StringType,
            required=False,
            description=(
                "(Optional.) Specifies the directory of API request caches."
                "When this is set, the cache will be used before calling to "
                "the external API endpoint. Any data not already cached will be "
                "recorded to this path as it is received."
            ),
        ),
        th.Property(
            "ignore_access_denied",
            th.BooleanType,
            required=False,
            default=False,
            description=(
                "True to ignore access denied (HTTP 401) errors. This option is "
                "provided for compatibility with prior versions of this tap, which "
                "would silently move on to the next stream after receiving an access "
                "denied error. If 'False', the tap will fail if access denied errors "
                "are received for any selected streams."
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams.

        The list of classes is generated automatically based on introspection.

        If any streams are disabled in settings, they will not be exposed here during
        discovery.
        """
        setup_requests_cache(dict(self.config), self.logger)

        stream_types: List[type] = []
        for _, module_class in inspect.getmembers(streams, inspect.isclass):
            if not issubclass(module_class, (GitLabStream)):
                continue  # Not a stream class.

            if module_class in [GitLabStream, ProjectBasedStream, GroupBasedStream]:
                continue  # Base classes, not streams.

            stream_name = module_class.name

            if stream_name in OPTIN_STREAM_NAMES and self.config.get(
                f"fetch_{stream_name}", False
            ):
                continue  # This is an "optin" class, and is not opted in.

            if stream_name in ULTIMATE_LICENSE_STREAM_NAMES and not self.config.get(
                "ultimate_license", False
            ):
                continue  # This is an ultimate license class and will be skipped.

            if issubclass(module_class, (ProjectBasedStream)) and not self.config.get(
                "projects", None
            ):
                continue  # No project IDs provided.

            if issubclass(module_class, (GroupBasedStream)) and not self.config.get(
                "groups", None
            ):
                continue  # No group IDs provided.

            stream_types.append(module_class)

        return [stream_class(tap=self) for stream_class in stream_types]
