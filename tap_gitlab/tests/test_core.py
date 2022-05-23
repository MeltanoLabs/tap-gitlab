"""Tests standard tap features using the built-in SDK tests library."""

import os
from typing import Any, Dict

from dotenv import load_dotenv
from singer_sdk.testing import get_standard_tap_tests

from tap_gitlab.tap import OPTIN_STREAM_NAMES, TapGitLab

load_dotenv()  # Import any environment variables from local `.env` file.

PREFIX = "TAP_GITLAB_"
SAMPLE_CONFIG: Dict[str, Any] = {
    # To improve test performance, optionally bump date forward or use a dynamic date:
    # "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "start_date": "2022-03-01T00:00:00Z",
    "private_token": os.getenv("TAP_GITLAB_PRIVATE_TOKEN"),
    "projects": os.getenv("TAP_GITLAB_PROJECTS", "meltano/demo-project"),
    "groups": os.getenv("TAP_GITLAB_GROUPS", "meltano/infra"),
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


def test_pkeys_in_schema():
    """Verify that primary keys are actually in the stream's schema."""
    tap = TapGitLab(config=SAMPLE_CONFIG, parse_env_config=True)
    for name, stream in tap.streams.items():
        pkeys = stream.primary_keys or []
        schema_props = set(stream.schema["properties"].keys())
        for pkey in pkeys:
            assert (
                pkey in schema_props
            ), f"Coding error in stream {name}: primary_key {pkey} is missing in schema"


def test_state_partitioning_keys_in_schema():
    """Verify that state partitioning keys are actually in the stream's schema."""
    tap = TapGitLab(config=SAMPLE_CONFIG, parse_env_config=True)
    for name, stream in tap.streams.items():
        pkeys = stream.state_partitioning_keys or []
        schema_props = set(stream.schema["properties"].keys())
        for pkey in pkeys:
            assert pkey in schema_props, (
                f"Coding error in stream {name}: state_partitioning_key "
                f"{pkey} is missing in schema"
            )
