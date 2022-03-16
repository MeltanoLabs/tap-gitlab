"""Utility functions."""
from typing import Any, Dict, List


def object_array_to_id_array(items: List[dict]) -> List[int]:
    """Extract id from nested array of objects."""
    return [item["id"] for item in items]


def pop_nested_id(record: Dict[str, Any], key: str) -> List[int]:
    """Extract id from nested owner object and removes the object."""
    return (record.pop("owner", None) or {}).pop("id", None)
