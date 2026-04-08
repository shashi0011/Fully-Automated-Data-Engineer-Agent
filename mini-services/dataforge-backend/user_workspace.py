import json
import os
import re
from typing import Any, Dict

from agent.utils import (
    BASE_PATH,
    RAW_DATA_DIR,
    CLEAN_DATA_DIR,
    REPORTS_DIR,
    WAREHOUSE_DB_PATH,
    SCHEMA_CACHE_PATH,
)

_SAFE_USER_RE = re.compile(r"^[a-zA-Z0-9_-]{3,80}$")


def normalize_user_id(user_id: str | None) -> str | None:
    if not user_id:
        return None
    candidate = str(user_id).strip()
    if not candidate:
        return None
    if not _SAFE_USER_RE.match(candidate):
        return None
    return candidate


def get_user_workspace(user_id: str | None) -> Dict[str, str]:
    normalized = normalize_user_id(user_id)
    if not normalized:
        return {
            "user_id": "default",
            "root": BASE_PATH,
            "raw_dir": RAW_DATA_DIR,
            "clean_dir": CLEAN_DATA_DIR,
            "reports_dir": REPORTS_DIR,
            "warehouse_db": WAREHOUSE_DB_PATH,
            "schema_cache": SCHEMA_CACHE_PATH,
            "configs_dir": os.path.join(BASE_PATH, "warehouse"),
        }

    root = os.path.join(BASE_PATH, "tenants", normalized)
    raw_dir = os.path.join(root, "data", "raw")
    clean_dir = os.path.join(root, "data", "clean")
    reports_dir = os.path.join(root, "reports")
    warehouse_dir = os.path.join(root, "warehouse")
    configs_dir = os.path.join(root, "configs")

    for d in [raw_dir, clean_dir, reports_dir, warehouse_dir, configs_dir]:
        os.makedirs(d, exist_ok=True)

    return {
        "user_id": normalized,
        "root": root,
        "raw_dir": raw_dir,
        "clean_dir": clean_dir,
        "reports_dir": reports_dir,
        "warehouse_db": os.path.join(warehouse_dir, "warehouse.duckdb"),
        "schema_cache": os.path.join(configs_dir, "schema_cache.json"),
        "configs_dir": configs_dir,
    }


def load_user_schema(user_id: str | None) -> Dict[str, Any]:
    workspace = get_user_workspace(user_id)
    schema_path = workspace["schema_cache"]
    if os.path.exists(schema_path):
        try:
            with open(schema_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_user_schema(user_id: str | None, schema: Dict[str, Any]) -> None:
    workspace = get_user_workspace(user_id)
    schema_path = workspace["schema_cache"]
    os.makedirs(os.path.dirname(schema_path), exist_ok=True)
    with open(schema_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, default=str)


def resolve_user_relative_path(user_id: str | None, relative_path: str) -> str:
    workspace = get_user_workspace(user_id)
    root = os.path.realpath(workspace["root"])
    full = os.path.realpath(os.path.join(root, relative_path))
    if not full.startswith(root + os.sep):
        raise ValueError("Access denied: path outside user workspace")
    return full


def relative_to_user_root(user_id: str | None, absolute_path: str) -> str:
    workspace = get_user_workspace(user_id)
    root = os.path.realpath(workspace["root"])
    full = os.path.realpath(absolute_path)
    rel = os.path.relpath(full, root)
    return rel.replace("\\", "/")
