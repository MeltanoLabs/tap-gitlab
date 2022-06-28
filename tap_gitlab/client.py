"""REST client handling, including GitLabStream base class."""

from __future__ import annotations

import copy
import urllib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union, cast
from urllib.parse import urlparse

import requests
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.streams import RESTStream

API_TOKEN_KEY = "Private-Token"
API_TOKEN_SETTING_NAME = "private_token"

DEFAULT_API_URL = "https://gitlab.com/api/v4"

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


def _truthy(val: Any) -> bool:
    """Convert strings from env vars and settings to booleans."""
    if isinstance(val, str):
        if val.lower() in ["false", "0"]:
            return False

    # Convert val to bool
    return not not val  # pylint: disable=C0113


class GitLabStream(RESTStream):
    """GitLab stream class."""

    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$.X-Next-Page"
    extra_url_params: Optional[dict] = None
    bookmark_param_name = "since"
    _LOG_REQUEST_METRIC_URLS = True  # Okay to print in logs
    # sensitive_request_path = False  # TODO: Update SDK to accept this instead.

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings.

        If no path is provided, the base URL will be appended with `/api/v4`.
        E.g. 'https://gitlab.com' would become 'https://gitlab.com/api/v4'

        Note: trailing slashes ('/') are scrubbed prior to comparison, so that
        'https://gitlab.com` is equivalent to 'https://gitlab.com/' and
        'https://gitlab.com/api/v4' is equivalent to 'https://gitlab.com/api/v4/'.
        """
        # Remove trailing '/' from url base.
        result = self.config.get("api_url", DEFAULT_API_URL).rstrip("/")

        # If path part is not provided, append the v4 endpoint as default:
        # For example 'https://gitlab.com' => 'https://gitlab.com/api/v4'
        if not urlparse(result).path:
            result += "/api/v4"
        return result

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
        params: dict = copy.copy(self.extra_url_params or {})

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

    @staticmethod
    def _url_encode(val: Union[str, datetime, bool, int, List[str]]) -> str:
        """Encode the val argument as url-compatible string."""
        return urllib.parse.quote_plus(str(val))

    def get_url(self, context: Optional[dict]) -> str:
        """Get stream entity URL."""
        url = "".join([self.url_base, self.path or ""])
        vals = copy.copy(dict(self.config))
        vals.update(context or {})
        for key, val in vals.items():
            search_text = "".join(["{", key, "}"])
            if search_text in url:
                url = url.replace(search_text, self._url_encode(val))
                if "{project_path}" in search_text:
                    self.logger.debug(
                        f"Found project arg. URL is {url} after parsing "
                        f"input val '{val}' to '{self._url_encode(val)}'."
                    )

        return url

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Post process records."""
        result = super().post_process(row, context)
        del row
        if result is None:
            return None

        assert context is not None  # Tell linter that context is non-null

        for key, val in context.items():
            if key in self.schema.get("properties", {}) and key not in result:
                result[key] = val

        return result

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        Overrides the base class in order to ignore 401 access denied errors if the
        config value 'ignore_access_denied' is True.

        Args:
            response: A `requests.Response`_ object.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response
        """
        if (
            _truthy(self.config.get("ignore_access_denied", False))
            and response.status_code == 401
        ):
            self.logger.info(
                "Ignoring 401 access denied error "
                "('ignore_access_denied' setting is True)."
            )
            return

        super().validate_response(response)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows.

        We override this method in order to skip over any 'access_denied' (401) errors
        and avoid parsing those responses as records.

        Args:
            response: A raw `requests.Response`_ object.

        Yields:
            One item for every item found in the response.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response
        """
        if (
            _truthy(self.config.get("ignore_access_denied", False))
            and response.status_code == 401
        ):
            yield from ()

        yield from super().parse_response(response)


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
