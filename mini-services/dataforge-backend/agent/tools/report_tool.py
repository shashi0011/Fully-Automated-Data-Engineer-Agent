"""
DataForge AI - Report Tool (Dataset-Agnostic)
Report generation functionality - Works with ANY dataset.
Each dataset gets its own report file.
"""

import os
import duckdb
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional

from agent.utils import (
    BASE_PATH,
    WAREHOUSE_DB_PATH,
    REPORTS_DIR,
    SCHEMA_CACHE_PATH,
    load_schema,
    quote_identifier,
    validate_identifier,
    validate_sql,
    categorize_columns,
    QUERY_TIMEOUT,
)


class ReportTool:
    """Tool for generating reports - Dataset Agnostic"""

    def __init__(self):
        pass

    @staticmethod
    def _get_report_filename(schema: Dict) -> str:
        """Generate a dataset-specific report filename."""
        clean_table = schema.get('table_name', 'data_clean')
        base = clean_table.replace("_clean", "")
        return f"{base}_report.csv"

    @staticmethod
    def _get_report_path(schema: Dict) -> str:
        filename = ReportTool._get_report_filename(schema)
        return os.path.join(REPORTS_DIR, filename)

    async def generate(self, schema: Dict = None) -> str:
        """Generate a summary report based on detected schema."""
        if schema is None:
            schema = load_schema()

        if not schema or not schema.get("columns"):
            return "Error: No schema loaded. Upload and ingest data first."

        table_name = schema.get('table_name', 'data_clean')
        if not validate_identifier(table_name):
            return f"Error: Invalid table name: {table_name!r}"

        if not os.path.exists(WAREHOUSE_DB_PATH):
            return "Error: Warehouse not found. Run pipeline first."

        report_path = self._get_report_path(schema)
        os.makedirs(os.path.dirname(report_path), exist_ok=True)

        con = duckdb.connect(WAREHOUSE_DB_PATH)
        con.execute(f"SET timeout={QUERY_TIMEOUT * 1000}")

        quoted_table = quote_identifier(table_name)

        # Check if table exists
        try:
            con.execute(f"SELECT * FROM {quoted_table} LIMIT 1")
        except duckdb.Error:
            con.close()
            return f"Error: Table {table_name} not found. Run transformation first."

        col_cats = categorize_columns(schema.get('columns', {}))
        numeric_cols = col_cats['numeric']
        category_cols = col_cats['categorical']
        date_cols = col_cats['date']

        report_sql = None

        if category_cols and numeric_cols:
            group_col = category_cols[0]
            agg_col = numeric_cols[0]
            quoted_group = quote_identifier(group_col)
            quoted_agg = quote_identifier(agg_col)

            report_sql = f"""
                SELECT
                    {quoted_group},
                    COUNT(*) as record_count,
                    SUM({quoted_agg}) as total_{agg_col},
                    ROUND(AVG({quoted_agg}), 2) as avg_{agg_col},
                    MIN({quoted_agg}) as min_{agg_col},
                    MAX({quoted_agg}) as max_{agg_col}
                FROM {quoted_table}
                GROUP BY {quoted_group}
                ORDER BY total_{agg_col} DESC
            """
        elif category_cols:
            group_col = category_cols[0]
            quoted_group = quote_identifier(group_col)
            report_sql = f"""
                SELECT
                    {quoted_group},
                    COUNT(*) as record_count
                FROM {quoted_table}
                GROUP BY {quoted_group}
                ORDER BY record_count DESC
            """
        elif numeric_cols:
            agg_col = numeric_cols[0]
            quoted_agg = quote_identifier(agg_col)
            report_sql = f"""
                SELECT
                    COUNT(*) as record_count,
                    SUM({quoted_agg}) as total_{agg_col},
                    ROUND(AVG({quoted_agg}), 2) as avg_{agg_col},
                    MIN({quoted_agg}) as min_{agg_col},
                    MAX({quoted_agg}) as max_{agg_col}
                FROM {quoted_table}
            """
        else:
            report_sql = f"SELECT COUNT(*) as record_count FROM {quoted_table}"

        is_safe, reason = validate_sql(report_sql)
        if not is_safe:
            con.close()
            return f"Error: Report SQL validation failed: {reason}"

        try:
            report_df = con.execute(report_sql).fetchdf()
            report_df.to_csv(report_path, index=False)
            count = len(report_df)
            con.close()
        except duckdb.Error as e:
            con.close()
            return f"Error: Failed to generate report: {e}"

        filename = self._get_report_filename(schema)
        return f"Generated report with {count} rows saved to {filename}"
