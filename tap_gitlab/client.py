"""REST client handling, including GitLabStream base class."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import requests
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.streams import RESTStream

API_TOKEN_KEY = "Private-Token"
API_TOKEN_SETTING_NAME = "private_token"

DEFAULT_API_URL = "https://gitlab.com/api/v4"

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class GitLabStream(RESTStream):
    """GitLab stream class."""

    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$.X-Next-Page"
    extra_url_params: dict = {}
    bookmark_param_name = "since"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("api_url", DEFAULT_API_URL)

    @property
    def schema_filename(self) -> str:
        """Return the filename for the stream's schema."""
        return f"{self.name}.json"

    @property
    def schema_filepath(self) -> Path:
        """Return the filepath for the stream's schema."""
        return SCHEMAS_DIR / self.schema_filename

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object."""
        return APIKeyAuthenticator.create_for_stream(
            self,
            key=API_TOKEN_KEY,
            value=self.config[API_TOKEN_SETTING_NAME],
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        # If the class has extra default params, start with those:
        params: dict = self.extra_url_params

        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
            if self.is_timestamp_replication_key:
                params[self.bookmark_param_name] = self.get_starting_timestamp(context)

        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return token for identifying next page or None if not applicable."""
        return response.headers.get("X-Next-Page", None)


class ProjectBasedStream(GitLabStream):
    """Base class for streams that are keys based on project ID."""

    state_partitioning_keys = ["project_path"]


class GroupBasedStream(GitLabStream):
    """Base class for streams that are keys based on group ID."""

    state_partitioning_keys = ["group_path"]

    @property
    def partitions(self) -> List[dict]:
        """Return a list of partition key dicts (if applicable), otherwise None."""
        if "{group_path}" in self.path:
            if "groups" not in self.config:
                raise ValueError(
                    f"Missing `groups` setting which is required for the "
                    f"'{self.name}' stream."
                )

            return [
                {"group_path": id}
                for id in cast(list, self.config["groups"].split(" "))
            ]

        raise ValueError(
            "Could not detect partition type for Gitlab stream "
            f"'{self.name}' ({self.path}). "
            "Expected a URL path containing '{project_path}' or '{group_path}'. "
        )
