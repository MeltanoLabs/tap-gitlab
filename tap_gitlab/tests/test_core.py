"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import os
from typing import Any, Dict

from dotenv import load_dotenv
from singer_sdk.testing import get_standard_tap_tests

from tap_gitlab.tap import OPTIN_STREAM_NAMES, TapGitLab

load_dotenv()  # Import any environment variables from local `.env` file.

PREFIX = "TAP_GITLAB_"
SAMPLE_CONFIG: Dict[str, Any] = {
    "start_date": "2022-03-01T00:00:00Z",
    # "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "private_token": os.getenv("TAP_GITLAB_PRIVATE_TOKEN"),
    "projects": os.getenv("TAP_GITLAB_PROJECTS", "meltano/demo-project"),
    "groups": os.getenv("TAP_GITLAB_GROUPS", "meltano/infra"),
}
for k, v in os.environ.items():
    if k.startswith(PREFIX):
        if v.lower() == "false":
            os.environ.pop(k)
            SAMPLE_CONFIG[k.lstrip(PREFIX).lower()] = False
        elif v.lower() == "true":
            os.environ.pop(k)
            SAMPLE_CONFIG[k.lstrip(PREFIX).lower()] = True


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapGitLab, config=SAMPLE_CONFIG)
    for test in tests:
        test()


#       https://gitlab.com/meltano/sdk/-/merge_requests/265
def test_tap_config_defaults():
    """Run standard tap tests from the SDK."""
    tap = TapGitLab(config=SAMPLE_CONFIG, parse_env_config=True)
    for optin_stream in OPTIN_STREAM_NAMES:
        assert f"fetch_{optin_stream}" in tap.config
