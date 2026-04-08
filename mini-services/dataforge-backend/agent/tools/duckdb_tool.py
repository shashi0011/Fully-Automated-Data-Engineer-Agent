"""
DataForge AI - DuckDB Tool (Dataset-Agnostic)
Data warehouse operations using DuckDB - Works with ANY dataset
"""

import json
import os
from typing import Any, Dict, List

import duckdb
import pandas as pd

from .schema_detector import SchemaDetector
from agent.utils import (
    quote_identifier,
    safe_table_name,
    validate_identifier,
    validate_sql,
)
from user_workspace import get_user_workspace, load_user_schema, save_user_schema


class DuckDBTool:
    """Tool for DuckDB warehouse operations - Dataset Agnostic"""

    def __init__(self):
        self.schema_detector = SchemaDetector()
        self.current_data_file = None
        self.current_table_name = "data_clean"

    def _get_connection(self, user_id: str = None) -> duckdb.DuckDBPyConnection:
        workspace = get_user_workspace(user_id)
        db_path = workspace["warehouse_db"]
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        return duckdb.connect(db_path)

    def set_data_file(self, file_path: str) -> str:
        self.current_data_file = file_path
        return f"Data file set to: {file_path}"

    async def ingest_file(self, file_path: str = None, table_name: str = None, user_id: str = None) -> Dict[str, Any]:
        if file_path is None:
            file_path = self.current_data_file

        if file_path is None:
            return {"error": "No data file specified. Upload a file first."}

        if not os.path.exists(file_path):
            return {"error": f"File not found: {file_path}"}

        schema = await self.schema_detector.detect_schema_from_file(file_path)
        if "error" in schema:
            return schema

        filename = os.path.basename(file_path)
        base_name = os.path.splitext(filename)[0]
        normalized_base = base_name[:-6] if base_name.lower().endswith("_clean") else base_name
        raw_table = safe_table_name(f"{normalized_base}_raw")
        if table_name is None:
            clean_table = safe_table_name(f"{normalized_base}_clean")
        else:
            if not validate_identifier(table_name):
                return {"error": f"Invalid table name: {table_name!r}"}
            clean_table = table_name.lower()

        try:
            if file_path.endswith(".csv"):
                df = pd.read_csv(file_path)
            elif file_path.endswith(".json"):
                df = pd.read_json(file_path)
            else:
                return {"error": "Unsupported file format. Use CSV or JSON."}
        except (pd.errors.EmptyDataError, pd.errors.ParserError, ValueError) as exc:
            return {"error": f"Failed to read file: {exc}"}

        con = self._get_connection(user_id=user_id)
        try:
            quoted_raw = quote_identifier(raw_table)
            con.execute(f"CREATE OR REPLACE TABLE {quoted_raw} AS SELECT * FROM df")
            count = con.execute(f"SELECT COUNT(*) FROM {quoted_raw}").fetchone()[0]
        except duckdb.Error as exc:
            con.close()
            return {"error": f"Failed to ingest data: {exc}"}
        con.close()

        schema["raw_table"] = raw_table
        schema["table_name"] = clean_table
        schema["source_file"] = file_path
        save_user_schema(user_id, schema)

        self.current_table_name = clean_table

        return {
            "status": "success",
            "message": f"Ingested {count} rows into {raw_table} table",
            "schema": schema,
        }

    async def ingest(self) -> str:
        result = await self.ingest_file()
        if "error" in result:
            return result["error"]
        return result["message"]

    @staticmethod
    def _feature_importance(df: pd.DataFrame) -> List[Dict[str, Any]]:
        features: List[Dict[str, Any]] = []

        numeric_df = df.select_dtypes(include=["number"])
        if not numeric_df.empty:
            variances = numeric_df.var(numeric_only=True).fillna(0)
            corr = numeric_df.corr().abs().fillna(0)
            for col in numeric_df.columns:
                variance = float(variances.get(col, 0.0))
                mean_corr = float(corr[col].drop(labels=[col], errors="ignore").mean() if len(corr.columns) > 1 else 0.0)
                score = variance * 0.7 + mean_corr * 0.3
                features.append(
                    {
                        "feature": col,
                        "type": "numeric",
                        "variance": round(variance, 6),
                        "mean_abs_correlation": round(mean_corr, 6),
                        "importance_score": round(score, 6),
                    }
                )

        cat_df = df.select_dtypes(exclude=["number", "datetime", "datetimetz"])
        for col in cat_df.columns:
            unique_ratio = float(cat_df[col].nunique(dropna=True) / max(len(cat_df), 1))
            score = unique_ratio * 0.4 + (1 - abs(0.5 - unique_ratio)) * 0.2
            features.append(
                {
                    "feature": col,
                    "type": "categorical",
                    "cardinality_ratio": round(unique_ratio, 6),
                    "importance_score": round(float(score), 6),
                }
            )

        return sorted(features, key=lambda x: x.get("importance_score", 0), reverse=True)

    @staticmethod
    def _looks_like_datetime(series: pd.Series) -> bool:
        non_null = series.dropna()
        if non_null.empty:
            return False
        as_str = non_null.astype(str).str.strip()
        sample = as_str.head(min(200, len(as_str)))
        date_like_ratio = sample.str.contains(
            r"(?:\d{1,4}[-/]\d{1,2}[-/]\d{1,4}|\d{1,2}:\d{2}|[A-Za-z]{3,9}\s+\d{1,2})",
            regex=True,
        ).mean()
        return bool(date_like_ratio >= 0.6)

    @staticmethod
    def _normalize_textual_numbers(series: pd.Series) -> pd.Series:
        """Convert basic textual numbers (e.g., 'twenty one') to digits."""
        units = {
            "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
            "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
            "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
            "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19,
        }
        tens = {
            "twenty": 20, "thirty": 30, "forty": 40, "fifty": 50,
            "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90,
        }

        def convert_token(value: Any) -> Any:
            if not isinstance(value, str):
                return value
            s = value.strip().lower().replace("-", " ")
            if not s:
                return value
            parts = [p for p in s.split() if p]
            if len(parts) == 1:
                if parts[0] in units:
                    return str(units[parts[0]])
                if parts[0] in tens:
                    return str(tens[parts[0]])
                return value
            if len(parts) == 2 and parts[0] in tens and parts[1] in units:
                return str(tens[parts[0]] + units[parts[1]])
            return value

        return series.map(convert_token)

    async def transform(self, user_id: str = None) -> Dict[str, Any]:
        """Smart cleaning with minimal row removal + transformation log."""
        schema = load_user_schema(user_id)
        raw_table = schema.get("raw_table", "data_raw")
        clean_table = schema.get("table_name", "data_clean")

        if not validate_identifier(raw_table):
            return {"error": f"Invalid raw table name: {raw_table!r}"}
        if not validate_identifier(clean_table):
            return {"error": f"Invalid clean table name: {clean_table!r}"}

        con = self._get_connection(user_id=user_id)
        quoted_raw = quote_identifier(raw_table)
        quoted_clean = quote_identifier(clean_table)

        try:
            con.execute(f"SELECT * FROM {quoted_raw} LIMIT 1")
            df = con.execute(f"SELECT * FROM {quoted_raw}").fetchdf()
        except duckdb.Error:
            con.close()
            return {"error": f"Raw table {raw_table} not found. Run ingest first."}

        original_count = len(df)
        logs: List[Dict[str, Any]] = []
        feature_importance = self._feature_importance(df)[:25]
        importance_map = {x.get("feature"): float(x.get("importance_score", 0.0)) for x in feature_importance}
        importance_values = sorted(v for v in importance_map.values() if isinstance(v, (float, int)))
        median_importance = importance_values[len(importance_values) // 2] if importance_values else 0.0

        for col in df.columns:
            series = df[col]
            missing_before = int(series.isna().sum())
            if missing_before == 0:
                continue

            missing_ratio = float(missing_before / max(len(df), 1))
            col_importance = float(importance_map.get(col, 0.0))
            if missing_ratio > 0.75 and col_importance < median_importance:
                logs.append(
                    {
                        "column": col,
                        "operation": "missing_value_imputation",
                        "strategy": "skipped_low_priority_high_missing",
                        "missing_before": missing_before,
                        "missing_after": missing_before,
                        "fill_preview": None,
                    }
                )
                continue

            strategy = ""
            fill_value: Any = None

            if pd.api.types.is_numeric_dtype(series):
                skewness = float(series.dropna().skew()) if series.dropna().shape[0] > 2 else 0.0
                if abs(skewness) > 1:
                    fill_value = series.median()
                    strategy = "median"
                else:
                    fill_value = series.mean()
                    strategy = "mean"
                df[col] = series.fillna(fill_value)
            elif pd.api.types.is_datetime64_any_dtype(series):
                df[col] = series.ffill().bfill()
                strategy = "forward_backward_fill"
            else:
                non_null_count = int(series.notna().sum())
                schema_col = schema.get("columns", {}).get(col, {})
                semantic = str(schema_col.get("semantic", "")).lower()
                normalized_series = series
                if semantic in {"money", "count", "score", "percentage", "numeric", "number", "age"} or any(
                    token in col.lower() for token in ["age", "count", "qty", "quantity", "price", "amount", "score", "number"]
                ):
                    normalized_series = self._normalize_textual_numbers(series)
                coerced_num = pd.to_numeric(normalized_series, errors="coerce")
                numeric_ratio = float(coerced_num.notna().sum() / max(non_null_count, 1))
                if numeric_ratio >= 0.85 and non_null_count > 0:
                    skewness = float(coerced_num.dropna().skew()) if coerced_num.dropna().shape[0] > 2 else 0.0
                    if abs(skewness) > 1:
                        fill_value = coerced_num.median()
                        strategy = "numeric_coerce_median"
                    else:
                        fill_value = coerced_num.mean()
                        strategy = "numeric_coerce_mean"
                    df[col] = coerced_num.fillna(fill_value)
                    missing_after = int(df[col].isna().sum())
                    logs.append(
                        {
                            "column": col,
                            "operation": "missing_value_imputation",
                            "strategy": strategy,
                            "missing_before": missing_before,
                            "missing_after": missing_after,
                            "fill_preview": None if fill_value is None else str(fill_value)[:80],
                        }
                    )
                    continue

                if self._looks_like_datetime(series):
                    try:
                        parsed_dt = pd.to_datetime(series, errors="coerce", format="mixed")
                    except TypeError:
                        parsed_dt = pd.to_datetime(series, errors="coerce")
                    dt_ratio = float(parsed_dt.notna().sum() / max(len(parsed_dt), 1))
                else:
                    dt_ratio = 0.0

                if dt_ratio >= 0.7:
                    df[col] = parsed_dt.ffill().bfill()
                    strategy = "forward_backward_fill_datetime"
                else:
                    mode = series.mode(dropna=True)
                    if not mode.empty and str(mode.iloc[0]).strip() != "":
                        fill_value = mode.iloc[0]
                        strategy = "mode"
                    else:
                        fill_value = "Unknown"
                        strategy = "unknown"
                    df[col] = series.fillna(fill_value)

            missing_after = int(df[col].isna().sum())
            logs.append(
                {
                    "column": col,
                    "operation": "missing_value_imputation",
                    "strategy": strategy,
                    "missing_before": missing_before,
                    "missing_after": missing_after,
                    "fill_preview": None if fill_value is None else str(fill_value)[:80],
                }
            )

        all_null_rows = df.isna().all(axis=1)
        removed_all_null = int(all_null_rows.sum())
        if removed_all_null > 0:
            df = df.loc[~all_null_rows].copy()
            logs.append(
                {
                    "operation": "row_removal",
                    "reason": "all_columns_null",
                    "rows_removed": removed_all_null,
                }
            )

        pre_dedup = len(df)
        df = df.drop_duplicates().copy()
        removed_dup = pre_dedup - len(df)
        if removed_dup > 0:
            logs.append({"operation": "deduplicate", "rows_removed": removed_dup})

        con.register("df_clean", df)
        try:
            con.execute(f"CREATE OR REPLACE TABLE {quoted_clean} AS SELECT * FROM df_clean")
        except duckdb.Error as exc:
            con.close()
            return {"error": f"Transformation failed: {exc}"}
        con.close()

        workspace = get_user_workspace(user_id)
        clean_data_path = os.path.join(workspace["clean_dir"], f"{clean_table}.csv")
        os.makedirs(os.path.dirname(clean_data_path), exist_ok=True)
        df.to_csv(clean_data_path, index=False)

        feature_importance = self._feature_importance(df)[:25]
        rows_removed = original_count - len(df)
        transform_log_path = os.path.join(workspace["configs_dir"], "transformation_log.json")
        with open(transform_log_path, "w", encoding="utf-8") as log_file:
            json.dump(
                {
                    "table": clean_table,
                    "original_count": original_count,
                    "row_count": len(df),
                    "rows_removed": rows_removed,
                    "steps": logs,
                    "feature_importance": feature_importance,
                },
                log_file,
                indent=2,
                default=str,
            )

        schema["row_count"] = len(df)
        schema["original_row_count"] = original_count
        schema["rows_removed"] = rows_removed
        schema["cleaning_applied"] = [f"{x.get('column', x.get('operation'))}: {x.get('strategy', x.get('reason', 'done'))}" for x in logs]
        schema["transformation_log"] = logs
        schema["transformation_log_file"] = transform_log_path
        schema["feature_importance"] = feature_importance
        save_user_schema(user_id, schema)

        return {
            "status": "success",
            "message": f"Cleaned {original_count} -> {len(df)} rows ({rows_removed} removed).",
            "output_file": clean_data_path,
            "row_count": len(df),
            "original_count": original_count,
            "rows_removed": rows_removed,
            "cleaning_operations": logs,
            "feature_importance": feature_importance,
            "llm_cleaning_used": False,
            "cleaning_plan_notes": [],
        }

    async def query(self, sql: str, user_id: str = None) -> Dict[str, Any]:
        is_safe, reason = validate_sql(sql)
        if not is_safe:
            return {"status": "error", "message": f"SQL validation failed: {reason}"}

        con = self._get_connection(user_id=user_id)
        try:
            result = con.execute(sql).fetchall()
            columns = [desc[0] for desc in con.description]
            con.close()
            return {
                "status": "success",
                "columns": columns,
                "data": [dict(zip(columns, row)) for row in result],
                "row_count": len(result),
            }
        except duckdb.Error as exc:
            con.close()
            return {"status": "error", "message": str(exc)}

    async def get_schema(self, table_name: str = None, user_id: str = None) -> Dict[str, Any]:
        con = self._get_connection(user_id=user_id)
        try:
            if table_name:
                if not validate_identifier(table_name):
                    con.close()
                    return {"error": f"Invalid table name: {table_name!r}"}

                quoted_table = quote_identifier(table_name)
                columns = con.execute(f"DESCRIBE {quoted_table}").fetchall()
                sample = con.execute(f"SELECT * FROM {quoted_table} LIMIT 3").fetchdf().to_dict("records")
                count = con.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0]
                con.close()
                return {
                    "table": table_name,
                    "row_count": count,
                    "columns": [{"name": col[0], "type": col[1]} for col in columns],
                    "sample_data": sample,
                }

            tables = con.execute("SHOW TABLES").fetchall()
            con.close()
            return {
                "tables": [t[0] for t in tables],
                "current_schema": load_user_schema(user_id),
            }
        except duckdb.Error as exc:
            con.close()
            return {"error": str(exc)}

    async def get_current_schema(self, user_id: str = None) -> Dict[str, Any]:
        return load_user_schema(user_id)

    async def create_table(self, table_name: str, schema: Dict) -> str:
        if not validate_identifier(table_name):
            return f"Error: Invalid table name: {table_name!r}"

        quoted_table = quote_identifier(table_name)
        quoted_columns = []
        for name, dtype in schema.items():
            if not validate_identifier(name):
                return f"Error: Invalid column name: {name!r}"
            if not validate_identifier(str(dtype)):
                return f"Error: Invalid type for column {name}: {dtype!r}"
            quoted_columns.append(f"{quote_identifier(name)} {quote_identifier(str(dtype))}")

        columns_sql = ", ".join(quoted_columns)
        con = self._get_connection()
        try:
            con.execute(f"CREATE TABLE IF NOT EXISTS {quoted_table} ({columns_sql})")
            con.close()
            return f"Table {table_name} created successfully"
        except duckdb.Error as exc:
            con.close()
            return f"Error creating table: {exc}"

    async def export_table(self, table_name: str, output_path: str) -> str:
        if not validate_identifier(table_name):
            return f"Error: Invalid table name: {table_name!r}"

        quoted_table = quote_identifier(table_name)
        con = self._get_connection()
        try:
            df = con.execute(f"SELECT * FROM {quoted_table}").fetchdf()
            con.close()
        except duckdb.Error as exc:
            con.close()
            return f"Error exporting table: {exc}"

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        return f"Exported {len(df)} rows to {output_path}"

    async def list_tables(self, user_id: str = None) -> List[str]:
        con = self._get_connection(user_id=user_id)
        try:
            tables = con.execute("SHOW TABLES").fetchall()
            con.close()
            return [t[0] for t in tables]
        except duckdb.Error:
            con.close()
            return []

    async def get_sample_data(self, table_name: str = None, limit: int = 10, user_id: str = None) -> Dict[str, Any]:
        if table_name is None:
            table_name = load_user_schema(user_id).get("table_name", "data_clean")

        if not validate_identifier(table_name):
            return {"error": f"Invalid table name: {table_name!r}"}

        try:
            limit = int(limit)
            if limit < 1 or limit > 1000:
                return {"error": "Limit must be between 1 and 1000"}
        except (TypeError, ValueError):
            return {"error": "Limit must be a valid integer"}

        quoted_table = quote_identifier(table_name)
        con = self._get_connection(user_id=user_id)
        try:
            df = con.execute(f"SELECT * FROM {quoted_table} LIMIT {limit}").fetchdf()
            con.close()
            return {
                "table": table_name,
                "columns": list(df.columns),
                "data": df.to_dict("records"),
                "row_count": len(df),
            }
        except duckdb.Error as exc:
            con.close()
            return {"error": str(exc)}
