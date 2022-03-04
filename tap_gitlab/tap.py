"""GitLab tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_gitlab.caching import setup_requests_cache
from tap_gitlab.streams import (
    # TODO: Import your custom stream types here:
    GitLabStream,
    ProjectsStream,
)
STREAM_TYPES = [
    # TODO: Compile a list of custom stream types here:
    ProjectsStream,
]


class TapGitLab(Tap):
    """GitLab tap class."""
    name = "tap-gitlab"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            required=False,
            description="Overrides the base URL.",
        ),
        th.Property(
            "private_token",
            th.StringType,
            required=True,
            description="TODO",
        ),
        th.Property(
            "groups",
            th.StringType,
            required=False,
            description="TODO",
        ),
        th.Property(
            "projects",
            th.StringType,
            required=False,
            description="TODO",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            description="TODO",
        ),
        th.Property(
            "ultimate_license",
            th.BooleanType,
            required=False,
            description="TODO",
        ),
        th.Property(
            "fetch_merge_request_commits",
            th.BooleanType,
            required=False,
            description="TODO",
            default=False,
        ),
        th.Property(
            "fetch_pipelines_extended",
            th.BooleanType,
            required=False,
            description="TODO",
            default=False,
        ),
        th.Property(
            "fetch_group_variables",
            th.BooleanType,
            required=False,
            description="TODO",
            default=False,
        ),
        th.Property(
            "fetch_project_variables",
            th.BooleanType,
            required=False,
            description="TODO",
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
        setup_requests_cache(self.config)
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
