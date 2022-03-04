"""REST client handling, including GitLabStream base class."""

from __future__ import annotations

from pathlib import Path

from typing import Any, Dict, List, cast, Optional

from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIKeyAuthenticator


API_TOKEN_KEY = "Private-Token"
API_TOKEN_SETTING_NAME = "private_token"

DEFAULT_API_URL = "https://gitlab.com/api/v4"

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class GitLabStream(RESTStream):
    """GitLab stream class."""

    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$.X-Next-Page"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("api_url", DEFAULT_API_URL)

    @property
    def schema_filepath(self) -> Path:
        return SCHEMAS_DIR / f"{self.name}.json"

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
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params


class ProjectBasedStream(GitLabStream):
    """Base class for streams that are keys based on project ID."""

    @property
    def partitions(self) -> List[dict]:
        """Return a list of partition key dicts (if applicable), otherwise None."""
        if "{project_id}" in self.path:
            return [
                {"project_id": id} for id in cast(list, self.config.get("project_ids"))
            ]

        if "{group_id}" in self.path:
            if "group_ids" not in self.config:
                raise ValueError(
                    f"Missing `group_ids` setting which is required for the "
                    f"'{self.name}' stream."
                )

            return [{"group_id": id} for id in cast(list, self.config.get("group_ids"))]

        raise ValueError(
            "Could not detect partition type for Gitlab stream "
            f"'{self.name}' ({self.path}). "
            "Expected a URL path containing '{project_id}' or '{group_id}'. "
        )
