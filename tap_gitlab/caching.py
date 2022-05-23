"""Test suite for tap-github."""

import logging
import os

import requests_cache

from tap_gitlab.client import API_TOKEN_KEY


def setup_requests_cache(tap_config: dict, logger: logging.Logger) -> None:
    """Install the caching mechanism for requests."""
    cache_path_root = tap_config.get("requests_cache_path", None)
    if not cache_path_root:
        return

    num_files = 0
    if os.path.exists(cache_path_root):
        num_files = len(os.listdir(cache_path_root))

    logger.info(
        f"Request caching is enabled at '{cache_path_root}'. "
        f"Found {num_files:,} cache resources during setup."
    )

    requests_cache.install_cache(
        cache_path_root,
        backend="filesystem",
        serializer="yaml",
        expire_after=24 * 60 * 60,
        # Important: make sure that API keys don't end up being cached:
        ignored_parameters=[API_TOKEN_KEY],
    )
