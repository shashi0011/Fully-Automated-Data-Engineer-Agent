"""
DataForge AI - Shared Utilities
Central place for constants, schema loading, SQL safety, column categorization.
"""
import os
import json
import re
import threading
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime

# === PATHS (parameterized, not hardcoded) ===
BASE_PATH = os.getenv("DATAFORGE_BASE_PATH", os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
DATA_DIR = os.path.join(BASE_PATH, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
CLEAN_DATA_DIR = os.path.join(DATA_DIR, "clean")
SAMPLES_DIR = os.path.join(DATA_DIR, "samples")
WAREHOUSE_DIR = os.path.join(BASE_PATH, "warehouse")
SCHEMA_CACHE_PATH = os.path.join(WAREHOUSE_DIR, "schema_cache.json")
WAREHOUSE_DB_PATH = os.path.join(WAREHOUSE_DIR, "warehouse.duckdb")
PIPELINES_DIR = os.path.join(BASE_PATH, "pipelines")
REPORTS_DIR = os.path.join(BASE_PATH, "reports")
DBT_DIR = os.path.join(BASE_PATH, "dbt_project")

# === QUERY TIMEOUT (seconds) ===
QUERY_TIMEOUT = 30

# === SCHEMA MANAGEMENT (with thread-safe locking) ===
_schema_lock = threading.Lock()


def load_schema() -> Dict[str, Any]:
    """Load schema from cache file with thread-safe locking."""
    with _schema_lock:
        try:
            if os.path.exists(SCHEMA_CACHE_PATH):
                with open(SCHEMA_CACHE_PATH, 'r') as f:
                    return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"[utils] Warning: Failed to load schema cache: {e}")
    return {}


def save_schema(schema: Dict[str, Any]) -> None:
    """Save schema to cache file with thread-safe locking."""
    with _schema_lock:
        try:
            os.makedirs(os.path.dirname(SCHEMA_CACHE_PATH), exist_ok=True)
            with open(SCHEMA_CACHE_PATH, 'w') as f:
                json.dump(schema, f, indent=2, default=str)
        except IOError as e:
            print(f"[utils] Error: Failed to save schema cache: {e}")


# === SQL SAFETY ===
# Regex for valid SQL identifiers (table names, column names)
VALID_IDENTIFIER_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]{0,127}$')


def validate_identifier(name: str) -> bool:
    """Validate that a string is a safe SQL identifier (prevents injection)."""
    if not name or not isinstance(name, str):
        return False
    return bool(VALID_IDENTIFIER_RE.match(name))


def quote_identifier(name: str) -> str:
    """Quote a SQL identifier using double quotes (DuckDB standard)."""
    if not validate_identifier(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return f'"{name}"'


def safe_table_name(filename: str) -> str:
    """Convert a filename to a safe SQL table name."""
    # Remove extension
    base = os.path.splitext(filename)[0]
    # Replace non-alphanumeric with underscore
    safe = re.sub(r'[^a-zA-Z0-9]', '_', base)
    # Remove leading digits
    safe = re.sub(r'^[0-9]+', '', safe)
    # Ensure not empty
    if not safe:
        safe = "data_table"
    return safe.lower()


def validate_sql(sql: str) -> Tuple[bool, str]:
    """Basic SQL validation to prevent dangerous operations.
    Returns (is_safe, reason).
    """
    dangerous_patterns = [
        (r'\bDROP\s+TABLE\b', "DROP TABLE is not allowed"),
        (r'\bDELETE\s+FROM\b', "DELETE FROM is not allowed"),
        (r'\bTRUNCATE\b', "TRUNCATE is not allowed"),
        (r'\bALTER\s+TABLE\b', "ALTER TABLE is not allowed"),
        (r'\bCREATE\s+DATABASE\b', "CREATE DATABASE is not allowed"),
        (r'\bGRANT\b', "GRANT is not allowed"),
        (r'\bREVOKE\b', "REVOKE is not allowed"),
        (r'\bATTACH\b', "ATTACH is not allowed"),
        (r'\bCOPY\s+.*\bFROM\s+.*http', "COPY FROM URL is not allowed"),
        (r';\s*\w', "Multiple statements are not allowed"),
    ]
    sql_upper = sql.upper()
    for pattern, reason in dangerous_patterns:
        if re.search(pattern, sql_upper, re.IGNORECASE):
            return False, reason
    return True, "OK"


# === COLUMN CATEGORIZATION ===
def categorize_columns(columns: Dict[str, Any]) -> Dict[str, List[str]]:
    """Categorize columns into types based on their detected schema info.

    Args:
        columns: Dict of {col_name: {type: str, semantic: str, ...}}

    Returns:
        Dict with keys: numeric, categorical, date, text, id
    """
    numeric_cols = []
    categorical_cols = []
    date_cols = []
    text_cols = []
    id_cols = []

    for col_name, col_info in columns.items():
        col_type = col_info.get("type", "").upper()
        semantic = col_info.get("semantic", "").lower()
        col_lower = col_name.lower()

        # ID columns
        if any(kw in col_lower for kw in ["_id", "id_", "uuid", "identifier"]):
            id_cols.append(col_name)
        # Date columns
        elif "DATE" in col_type or "TIME" in col_type or "TIMESTAMP" in col_type:
            date_cols.append(col_name)
        # Numeric columns
        elif any(t in col_type for t in ["INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL", "BIGINT", "SMALLINT"]):
            if any(kw in semantic for kw in ["category", "type", "status", "gender", "department"]):
                categorical_cols.append(col_name)
            else:
                numeric_cols.append(col_name)
        # Categorical columns
        elif any(kw in semantic for kw in ["category", "type", "status", "gender", "department", "region", "country", "name"]):
            categorical_cols.append(col_name)
        # Text columns
        elif any(t in col_type for t in ["VARCHAR", "TEXT", "STRING", "CHAR"]):
            text_cols.append(col_name)
        else:
            # Fallback: try to infer from type string
            if any(t in col_type for t in ["INT", "FLOAT", "DOUBLE", "NUMERIC"]):
                numeric_cols.append(col_name)
            else:
                text_cols.append(col_name)

    return {
        "numeric": numeric_cols,
        "categorical": categorical_cols,
        "date": date_cols,
        "text": text_cols,
        "id": id_cols,
    }


def format_result(success: bool, message: str, data: Any = None, files: List[str] = None, logs: List[str] = None, duration: float = None) -> Dict[str, Any]:
    """Standardized result format for all agent operations."""
    return {
        "status": "success" if success else "error",
        "message": message,
        "data": data,
        "files": files or [],
        "logs": logs or [],
        "duration": duration,
        "timestamp": datetime.utcnow().isoformat(),
    }
