"""Test suite for tap-github."""

import requests_cache


def setup_requests_cache(tap_config: dict) -> None:
    cache_path_root = tap_config.get("requests_cache_path", None)
    if not cache_path_root:
        return None

    # recording = tap_config.get("requests_recording_enabled", False)
    # TODO: leverage `recording` to enable/disable the below

    requests_cache.install_cache(
        cache_path_root,
        backend="filesystem",
        serializer="yaml",
        expire_after=24 * 60 * 60,
        # Important: make sure that API keys don't end up being cached:
        ignored_parameters=["x-api-key"],
    )
