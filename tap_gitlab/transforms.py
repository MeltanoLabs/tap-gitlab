from typing import Any, Dict, List


def object_array_to_id_array(items: List[dict]) -> List[int]:
    return [item["id"] for item in items]


def pop_nested_id(record: Dict[str, Any], key: str) -> List[int]:
    return (record.pop("owner", None) or {}).pop("id", None)
