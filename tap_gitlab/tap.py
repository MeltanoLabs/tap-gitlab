"""GitLab tap class."""

import inspect
from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_gitlab.caching import setup_requests_cache
from tap_gitlab.streams import GitLabStream, ProjectBasedStream
from tap_gitlab import streams

OPTIN_CLASS_NAMES = [
    "merge_request_commits",
    "pipelines_extended",
    "group_variables",
    "project_variables",
]
ULTIMATE_LICENSE_CLASS_NAMES = ["epics", "epic_issues"]


class TapGitLab(Tap):
    """GitLab tap class."""

    name = "tap-gitlab"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            required=False,
            description="Optionally overrides the default base URL for the Gitlab API.",
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
            "requests_cache_path",
            th.StringType,
            required=False,
            description=(
                "(Optional.) Specifies the directory of API request caches."
                "When this is set, the cache will be used before calling to "
                "the external API endpoint. If set and "
                "`requests_recording_enabled` is `True`, then API data will also be "
                "recorded as it is received."
            ),
        ),
        # TODO:
        # th.Property(
        #     "requests_recording_enabled",
        #     th.BooleanType,
        #     required=False,
        #     description=(
        #         "Set to `True` to enable recording to the requests cache. "
        #         "This setting is ignored if `requests_cache_path` is not set."
        #     ),
        #     default=False,
        # )
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        setup_requests_cache(dict(self.config))

        stream_types: List[type] = []
        for class_name, module_class in inspect.getmembers(streams, inspect.isclass):
            class_name = module_class.__name__
            if not issubclass(module_class, (GitLabStream, ProjectBasedStream)):
                continue  # Not a stream class.

            if class_name in ["GitLabStream", "ProjectBasedStream"]:
                continue  # Base classes, not streams.

            if (
                class_name in OPTIN_CLASS_NAMES
                and not self.config[f"fetch_{class_name}"]
            ):
                continue  # This is an "optin" class, and is not opted in.

            if (
                class_name in ULTIMATE_LICENSE_CLASS_NAMES
                and not self.config["ultimate_license"]
            ):
                continue  # This is an ultimate license class and will be skipped.

            stream_types.append(module_class)

        return [stream_class(tap=self) for stream_class in stream_types]
