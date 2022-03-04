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

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service"
        ),
        th.Property(
            "project_ids",
            th.ArrayType(th.StringType),
            required=True,
            description="Project IDs to replicate"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.mysample.com",
            description="The url for the API service"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
