"""Tests standard tap features using the built-in SDK tests library."""

import os
from typing import Any, Dict

from dotenv import load_dotenv
from singer_sdk.testing import get_tap_test_class

from tap_gitlab.streams import ProjectsStream
from tap_gitlab.tap import OPTIN_STREAM_NAMES, TapGitLab

load_dotenv()  # Import any environment variables from local `.env` file.

PREFIX = "TAP_GITLAB_"
SAMPLE_CONFIG: Dict[str, Any] = {
    # To improve test performance, optionally bump date forward or use a dynamic date:
    # "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "start_date": "2022-03-01T00:00:00Z",
    "private_token": os.getenv("TAP_GITLAB_PRIVATE_TOKEN"),
    "projects": os.getenv("TAP_GITLAB_PROJECTS", "meltano/demo-project"),
    "groups": os.getenv("TAP_GITLAB_GROUPS", "meltano"),
}

assert (
    SAMPLE_CONFIG["private_token"] is not None
), "Please set TAP_GITLAB_PRIVATE_TOKEN in your env vars before running tests"

for k, v in os.environ.items():
    if k.startswith(PREFIX):
        if v.lower() == "false":
            os.environ.pop(k)
            SAMPLE_CONFIG[k.lstrip(PREFIX).lower()] = False
        elif v.lower() == "true":
            os.environ.pop(k)
            SAMPLE_CONFIG[k.lstrip(PREFIX).lower()] = True


# Run standard built-in tap tests from the SDK:
TestTapStackExchange = get_tap_test_class(tap_class=TapGitLab, config=SAMPLE_CONFIG)


# https://gitlab.com/meltano/sdk/-/merge_requests/265
def test_tap_config_defaults():
    """Run standard tap tests from the SDK."""
    tap = TapGitLab(config=SAMPLE_CONFIG, parse_env_config=True)
    for optin_stream in OPTIN_STREAM_NAMES:
        assert f"fetch_{optin_stream}" in tap.config


def test_get_repo_ids():
    """Check that the "presync" graphql call returns clean repo names/ids."""
    tap = TapGitLab(config=SAMPLE_CONFIG, parse_env_config=True)
    stream = ProjectsStream(tap=tap)

    list_of_buggy_repos = [
        "meLTano/sDk",  # incorrect case
        "DoesNot/Exist",  # does not exist ;)
        "gitlab-org/graphql-sandbox",  # correct value
    ]
    clean_list = stream.get_repo_ids(list_of_buggy_repos)

    assert clean_list == [
        {"project_id": "22672923", "project_path": "meltano/sdk"},
        {"project_id": "15297693", "project_path": "gitlab-org/graphql-sandbox"},
    ]
