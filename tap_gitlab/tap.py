"""GitLab tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
# TODO: Import your custom stream types here:
from tap_gitlab.streams import (
    GitLabStream,
    UsersStream,
    GroupsStream,
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    UsersStream,
    GroupsStream,
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
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
