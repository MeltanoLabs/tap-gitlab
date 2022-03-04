"""Stream type classes for tap-gitlab."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_gitlab.client import GitLabStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class ProjectsStream(GitLabStream):
    """Define custom stream."""
    name = "projects"
    path = "/projects"
    primary_keys = ["id"]
    replication_key = "last_activity_at"
