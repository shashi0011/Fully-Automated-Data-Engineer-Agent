"""
DataForge AI - Report Tool (Dataset-Agnostic)
Report generation functionality - Works with ANY dataset
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
        self.report_path = os.path.join(REPORTS_DIR, "report.csv")

    async def generate(self) -> str:
        """Generate a summary report based on detected schema"""
        os.makedirs(os.path.dirname(self.report_path), exist_ok=True)

        if not os.path.exists(WAREHOUSE_DB_PATH):
            return "Error: Warehouse not found. Run pipeline first."

        schema = load_schema()
        table_name = schema.get('table_name', 'data_clean')

        if not validate_identifier(table_name):
            return f"Error: Invalid table name: {table_name!r}"

        con = duckdb.connect(WAREHOUSE_DB_PATH)
        con.execute(f"SET timeout={QUERY_TIMEOUT * 1000}")

        quoted_table = quote_identifier(table_name)

        # Check if table exists
        try:
            con.execute(f"SELECT * FROM {quoted_table} LIMIT 1")
        except duckdb.Error:
            con.close()
            return f"Error: Table {table_name} not found. Run transformation first."

        # Get column categories using shared utility
        col_cats = categorize_columns(schema.get('columns', {}))
        numeric_cols = col_cats['numeric']
        category_cols = col_cats['categorical']
        date_cols = col_cats['date']

        # Build dynamic report query with safe identifiers
        report_sql = None

        if category_cols and numeric_cols:
            # Group by first category column, aggregate first numeric column
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
            # Just count by category
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
            # Just aggregate numeric
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
            # Fallback: just count
            report_sql = f"SELECT COUNT(*) as record_count FROM {quoted_table}"

        # Validate SQL before executing
        is_safe, reason = validate_sql(report_sql)
        if not is_safe:
            con.close()
            return f"Error: Report SQL validation failed: {reason}"

        try:
            report_df = con.execute(report_sql).fetchdf()
            report_df.to_csv(self.report_path, index=False)
            count = len(report_df)
            con.close()
        except duckdb.Error as e:
            con.close()
            return f"Error: Failed to generate report: {e}"

        return f"Generated report with {count} rows saved to {self.report_path}"

    async def generate_summary(self) -> Dict[str, Any]:
        """Generate executive summary based on detected schema"""
        if not os.path.exists(WAREHOUSE_DB_PATH):
            return {"error": "Warehouse not found"}

        schema = load_schema()
        table_name = schema.get('table_name', 'data_clean')

        if not validate_identifier(table_name):
            return {"error": f"Invalid table name: {table_name!r}"}

        con = duckdb.connect(WAREHOUSE_DB_PATH)
        try:
            con.execute(f"SET timeout={QUERY_TIMEOUT * 1000}")
        except duckdb.CatalogException:
            pass

        quoted_table = quote_identifier(table_name)

        try:
            col_cats = categorize_columns(schema.get('columns', {}))
            numeric_cols = col_cats['numeric']
            category_cols = col_cats['categorical']

            summary = {
                "dataset_type": schema.get('dataset_type', 'generic'),
                "table_name": table_name,
                "generated_at": datetime.now().isoformat()
            }

            # Total records
            total_records = con.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0]
            summary["total_records"] = int(total_records)

            # Numeric aggregations
            if numeric_cols:
                agg_col = numeric_cols[0]
                quoted_agg = quote_identifier(agg_col)
                total = con.execute(f"SELECT SUM({quoted_agg}) FROM {quoted_table}").fetchone()[0]
                avg = con.execute(f"SELECT AVG({quoted_agg}) FROM {quoted_table}").fetchone()[0]
                summary[f"total_{agg_col}"] = float(total) if total else 0
                summary[f"avg_{agg_col}"] = float(avg) if avg else 0

            # Top categories
            if category_cols:
                group_col = category_cols[0]
                quoted_group = quote_identifier(group_col)
                top_sql = f"""
                    SELECT {quoted_group}, COUNT(*) as count
                    FROM {quoted_table}
                    GROUP BY {quoted_group}
                    ORDER BY count DESC
                    LIMIT 5
                """
                top_items = con.execute(top_sql).fetchdf().to_dict('records')
                summary[f"top_{group_col}"] = top_items

            con.close()
            return summary

        except duckdb.Error as e:
            con.close()
            return {"error": str(e)}

    async def generate_chart_data(self, chart_type: str = None) -> Dict[str, Any]:
        """Generate data for charts based on detected schema"""
        if not os.path.exists(WAREHOUSE_DB_PATH):
            return {"error": "Warehouse not found"}

        schema = load_schema()
        table_name = schema.get('table_name', 'data_clean')

        if not validate_identifier(table_name):
            return {"error": f"Invalid table name: {table_name!r}"}

        con = duckdb.connect(WAREHOUSE_DB_PATH)
        try:
             con.execute(f"SET timeout={QUERY_TIMEOUT * 1000}")
        except duckdb.CatalogException:
            pass

        quoted_table = quote_identifier(table_name)

        try:
            col_cats = categorize_columns(schema.get('columns', {}))
            numeric_cols = col_cats['numeric']
            category_cols = col_cats['categorical']
            date_cols = col_cats['date']

            charts = {}

            # Primary chart: Category distribution
            if category_cols and numeric_cols:
                group_col = category_cols[0]
                agg_col = numeric_cols[0]
                quoted_group = quote_identifier(group_col)
                quoted_agg = quote_identifier(agg_col)

                primary_sql = f"""
                    SELECT {quoted_group} as category, SUM({quoted_agg}) as value
                    FROM {quoted_table}
                    GROUP BY {quoted_group}
                    ORDER BY value DESC
                    LIMIT 10
                """
                charts["primary"] = con.execute(primary_sql).fetchdf().to_dict('records')

            # Secondary chart: Count by category
            if category_cols:
                idx = min(1, len(category_cols) - 1)
                group_col = category_cols[idx]
                quoted_group = quote_identifier(group_col)

                secondary_sql = f"""
                    SELECT {quoted_group} as category, COUNT(*) as value
                    FROM {quoted_table}
                    GROUP BY {quoted_group}
                    ORDER BY value DESC
                    LIMIT 10
                """
                charts["secondary"] = con.execute(secondary_sql).fetchdf().to_dict('records')

            # Trend chart: Over time
            if date_cols and numeric_cols:
                date_col = date_cols[0]
                agg_col = numeric_cols[0]
                quoted_date = quote_identifier(date_col)
                quoted_agg = quote_identifier(agg_col)

                trend_sql = f"""
                    SELECT strftime({quoted_date}, '%Y-%m') as period, SUM({quoted_agg}) as value
                    FROM {quoted_table}
                    GROUP BY period
                    ORDER BY period
                    LIMIT 12
                """
                charts["trend"] = con.execute(trend_sql).fetchdf().to_dict('records')

            con.close()
            return charts

        except duckdb.Error as e:
            con.close()
            return {"error": str(e)}
