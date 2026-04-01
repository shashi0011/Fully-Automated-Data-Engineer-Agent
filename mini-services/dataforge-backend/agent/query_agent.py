"""
DataForge AI - Query Agent (Dataset-Agnostic)
Natural Language to SQL conversion using schema detection
Works with ANY dataset - sales, news, medical, finance, etc.
"""

import os
import sys
import json
import duckdb
import time
import re
from datetime import datetime
from typing import Dict, Any, List, Optional

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.tools.schema_detector import SchemaDetector
from agent.utils import (
    WAREHOUSE_DB_PATH,
    SCHEMA_CACHE_PATH,
    load_schema,
    quote_identifier,
    validate_identifier,
    validate_sql,
    categorize_columns,
    QUERY_TIMEOUT,
)


class QueryAgent:
    """Agent for converting natural language to SQL - Dataset Agnostic"""

    def __init__(self):
        self.max_retries = 2
        self.schema_detector = SchemaDetector()

        # Load cached schema or use default
        self.schema = self._load_schema()

    def _load_schema(self) -> Dict[str, Any]:
        """Load schema from cache using shared utility"""
        schema = load_schema()
        if not schema:
            return {
                "table_name": "data_clean",
                "columns": {},
                "dataset_type": "generic",
                "suggested_queries": []
            }
        return schema

    def _refresh_schema(self):
        """Refresh schema from cache"""
        self.schema = self._load_schema()

    def _get_system_prompt(self) -> str:
        """Get the system prompt for SQL generation based on detected schema"""
        table_name = self.schema.get('table_name', 'data_clean')
        columns = self.schema.get('columns', {})
        dataset_type = self.schema.get('dataset_type', 'generic')

        column_desc = []
        for col_name, col_info in columns.items():
            semantic = col_info.get('semantic', 'generic')
            dtype = col_info.get('type', 'unknown')
            samples = col_info.get('sample_values', [])
            column_desc.append(f"- {col_name}: {dtype} ({semantic}) - Examples: {samples}")

        return f"""You are an expert data analyst and SQL generator.

Database: DuckDB
Table: {table_name}
Dataset Type: {dataset_type}

Schema:
{chr(10).join(column_desc)}

Rules:
1. Use ONLY the columns listed above - do NOT hallucinate columns
2. Use appropriate aggregation functions (SUM, AVG, COUNT, MAX, MIN)
3. Include ORDER BY when ranking or sorting is implied
4. Include LIMIT when user asks for "top N" or "bottom N"
5. Use strftime for date operations: strftime(date_column, '%Y-%m') for month
6. For percentage or ratio calculations, use proper arithmetic
7. Return ONLY the SQL query - no explanations, no markdown formatting
8. Do NOT use DROP, DELETE, UPDATE, INSERT, or ALTER statements
9. Always use valid DuckDB SQL syntax"""

    def _extract_sql(self, response: str) -> str:
        """Extract SQL from LLM response"""
        response = response.strip()

        if response.startswith("```"):
            lines = response.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].startswith("```"):
                lines = lines[:-1]
            response = "\n".join(lines).strip()

        if response.upper().startswith("SQL"):
            response = response[3:].strip()

        return response

    def _validate_sql(self, sql: str) -> Tuple[bool, str]:
        """Validate SQL for safety using shared utility"""
        return validate_sql(sql)

    def _find_column_by_keyword(self, question_lower: str, columns_info: Dict,
                                 semantic_types: List[str] = None) -> Optional[str]:
        """Find a column that matches keywords in the question"""
        # First, try exact column name match
        for col_name in columns_info.keys():
            if col_name.lower() in question_lower:
                return col_name

        # Then try semantic type match
        if semantic_types:
            for col_name, col_info in columns_info.items():
                if col_info.get('semantic') in semantic_types:
                    return col_name

        return None

    def _find_id_column(self, columns: Dict[str, Any]) -> Optional[str]:
        """Dynamically find an ID column from schema (replaces hardcoded patient_id)."""
        # Check categorize_columns result for id columns
        cats = categorize_columns(columns)
        if cats.get("id"):
            return cats["id"][0]

        # Fallback: check column names directly
        for col_name, col_info in columns.items():
            col_lower = col_name.lower()
            semantic = col_info.get("semantic", "").lower()
            if (any(kw in col_lower for kw in ["_id", "id_", "uuid", "identifier"])
                    or semantic == "id"):
                return col_name

        return None

    def _generate_sql_local(self, question: str) -> str:
        """Generate SQL locally using schema-aware pattern matching with safe identifiers"""
        question_lower = question.lower()

        # Refresh schema
        self._refresh_schema()

        table_name = self.schema.get('table_name', 'data_clean')
        columns = self.schema.get('columns', {})
        dataset_type = self.schema.get('dataset_type', 'generic')

        if not columns:
            # Fallback: query the database directly to get columns
            try:
                con = duckdb.connect(WAREHOUSE_DB_PATH, read_only=True)
                if validate_identifier(table_name):
                    cols = con.execute(f"DESCRIBE {quote_identifier(table_name)}").fetchall()
                    for col in cols:
                        columns[col[0]] = {"type": col[1], "semantic": "generic"}
                con.close()
            except duckdb.Error:
                pass

        if not columns:
            # No columns available, return simple query
            return f"SELECT * FROM {quote_identifier('data_clean')} LIMIT 20"

        # Use shared categorize_columns
        cats = categorize_columns(columns)
        numeric_cols = cats["numeric"]
        category_cols = cats["categorical"]
        date_cols = cats["date"]

        # Dynamic ID column detection (replaces hardcoded patient_id)
        id_col = self._find_id_column(columns)

        # Extract numbers from question
        limit = 10
        limit_match = re.search(r'top\s+(\d+)|first\s+(\d+)|limit\s+(\d+)', question_lower)
        if limit_match:
            limit = int(limit_match.group(1) or limit_match.group(2) or limit_match.group(3))

        # Determine aggregation function
        agg_func = "SUM"
        if 'average' in question_lower or 'avg' in question_lower or 'mean' in question_lower:
            agg_func = "AVG"
        elif 'count' in question_lower or 'how many' in question_lower:
            agg_func = "COUNT"
        elif 'max' in question_lower or 'highest' in question_lower or 'most' in question_lower or 'largest' in question_lower:
            agg_func = "MAX"
        elif 'min' in question_lower or 'lowest' in question_lower or 'least' in question_lower or 'smallest' in question_lower:
            agg_func = "MIN"

        # Find relevant columns from question
        target_col = self._find_column_by_keyword(question_lower, columns,
                                                   ['money', 'count', 'score'])
        group_col = self._find_column_by_keyword(question_lower, columns,
                                                  ['category', 'location', 'name'])
        date_col = date_cols[0] if date_cols else None

        # Quote all identifiers
        quoted_table = quote_identifier(table_name)
        quoted_target = quote_identifier(target_col) if target_col else None
        quoted_group = quote_identifier(group_col) if group_col else None
        quoted_date = quote_identifier(date_col) if date_col else None
        quoted_num0 = quote_identifier(numeric_cols[0]) if numeric_cols else None
        quoted_cat0 = quote_identifier(category_cols[0]) if category_cols else None

        # ============ PATTERN MATCHING ============

        # TOP N pattern
        if 'top' in question_lower or 'best' in question_lower or 'highest' in question_lower:
            if quoted_target and quoted_group:
                return f"SELECT {quoted_group}, {agg_func}({quoted_target}) as total_{target_col} FROM {quoted_table} GROUP BY {quoted_group} ORDER BY total_{target_col} DESC LIMIT {limit}"
            elif quoted_target:
                return f"SELECT {quoted_target}, {agg_func}({quoted_target}) as total FROM {quoted_table} GROUP BY {quoted_target} ORDER BY total DESC LIMIT {limit}"
            elif quoted_group:
                return f"SELECT {quoted_group}, COUNT(*) as count FROM {quoted_table} GROUP BY {quoted_group} ORDER BY count DESC LIMIT {limit}"
            elif quoted_num0:
                return f"SELECT *, {quoted_num0} FROM {quoted_table} ORDER BY {quoted_num0} DESC LIMIT {limit}"

        # BOTTOM N pattern
        if 'bottom' in question_lower or 'worst' in question_lower or 'lowest' in question_lower:
            if quoted_target and quoted_group:
                return f"SELECT {quoted_group}, {agg_func}({quoted_target}) as total_{target_col} FROM {quoted_table} GROUP BY {quoted_group} ORDER BY total_{target_col} ASC LIMIT {limit}"
            elif quoted_target:
                return f"SELECT {quoted_target}, {agg_func}({quoted_target}) as total FROM {quoted_table} GROUP BY {quoted_target} ORDER BY total ASC LIMIT {limit}"

        # TREND over time
        if 'trend' in question_lower or 'over time' in question_lower or 'monthly' in question_lower or 'daily' in question_lower or 'yearly' in question_lower:
            if quoted_date:
                if quoted_target:
                    period = '%Y-%m' if 'monthly' in question_lower else '%Y-%m-%d' if 'daily' in question_lower else '%Y'
                    return f"SELECT strftime({quoted_date}, '{period}') as period, {agg_func}({quoted_target}) as value FROM {quoted_table} GROUP BY period ORDER BY period"
                else:
                    return f"SELECT strftime({quoted_date}, '%Y-%m') as period, COUNT(*) as count FROM {quoted_table} GROUP BY period ORDER BY period"

        # GROUP BY pattern
        if 'by' in question_lower and quoted_group:
            if quoted_target:
                return f"SELECT {quoted_group}, {agg_func}({quoted_target}) as {agg_func.lower()}_{target_col} FROM {quoted_table} GROUP BY {quoted_group} ORDER BY {agg_func.lower()}_{target_col} DESC"
            else:
                return f"SELECT {quoted_group}, COUNT(*) as count FROM {quoted_table} GROUP BY {quoted_group} ORDER BY count DESC"

        # TOTAL/SUM pattern
        if 'total' in question_lower or 'sum' in question_lower:
            if quoted_target:
                return f"SELECT SUM({quoted_target}) as total_{target_col}, COUNT(*) as record_count FROM {quoted_table}"
            elif quoted_num0:
                return f"SELECT SUM({quoted_num0}) as total, COUNT(*) as record_count FROM {quoted_table}"

        # AVERAGE pattern
        if 'average' in question_lower or 'avg' in question_lower or 'mean' in question_lower:
            if quoted_target:
                return f"SELECT AVG({quoted_target}) as avg_{target_col} FROM {quoted_table}"
            elif quoted_num0:
                return f"SELECT AVG({quoted_num0}) as avg FROM {quoted_table}"

        # COUNT pattern
        if 'count' in question_lower or 'how many' in question_lower:
            if 'unique' in question_lower or 'distinct' in question_lower:
                if quoted_target:
                    return f"SELECT COUNT(DISTINCT {quoted_target}) as unique_count FROM {quoted_table}"
                elif quoted_cat0:
                    return f"SELECT COUNT(DISTINCT {quoted_cat0}) as unique_count FROM {quoted_table}"
            if quoted_group:
                return f"SELECT {quoted_group}, COUNT(*) as count FROM {quoted_table} GROUP BY {quoted_group} ORDER BY count DESC"
            return f"SELECT COUNT(*) as total_count FROM {quoted_table}"

        # SHOW/LIST pattern
        if 'show' in question_lower or 'list' in question_lower or 'display' in question_lower:
            if 'all' in question_lower:
                return f"SELECT * FROM {quoted_table} LIMIT {limit}"
            if quoted_group:
                return f"SELECT DISTINCT {quoted_group} FROM {quoted_table} LIMIT {limit}"
            return f"SELECT * FROM {quoted_table} LIMIT {limit}"

        # COMPARE pattern
        if 'compare' in question_lower or 'versus' in question_lower or ' vs ' in question_lower:
            if quoted_cat0 and quoted_num0:
                return f"SELECT {quoted_cat0}, {agg_func}({quoted_num0}) as total FROM {quoted_table} GROUP BY {quoted_cat0} ORDER BY total DESC"

        # DISTRIBUTION pattern
        if 'distribution' in question_lower or 'breakdown' in question_lower:
            if quoted_group:
                return f"SELECT {quoted_group}, COUNT(*) as count, ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {quoted_table}), 2) as percentage FROM {quoted_table} GROUP BY {quoted_group} ORDER BY count DESC"

        # DATASET-SPECIFIC PATTERNS

        # Sales-specific
        if dataset_type == "sales":
            if 'revenue' in question_lower or 'sales' in question_lower:
                for col in numeric_cols:
                    if 'revenue' in col.lower() or 'sales' in col.lower():
                        quoted_col = quote_identifier(col)
                        if quoted_group:
                            return f"SELECT {quoted_group}, SUM({quoted_col}) as total FROM {quoted_table} GROUP BY {quoted_group} ORDER BY total DESC"
                        return f"SELECT SUM({quoted_col}) as total_revenue FROM {quoted_table}"

        # News-specific
        if dataset_type == "news":
            if 'recent' in question_lower or 'latest' in question_lower:
                if quoted_date:
                    return f"SELECT * FROM {quoted_table} ORDER BY {quoted_date} DESC LIMIT {limit}"
            if 'article' in question_lower:
                return f"SELECT * FROM {quoted_table} LIMIT {limit}"

        # Medical-specific — uses dynamic ID column instead of hardcoded patient_id
        if dataset_type == "medical":
            if 'patient' in question_lower:
                if quoted_group:
                    return f"SELECT {quoted_group}, COUNT(*) as patient_count FROM {quoted_table} GROUP BY {quoted_group} ORDER BY patient_count DESC"
                # Use dynamic ID column detection instead of hardcoded patient_id
                if id_col:
                    quoted_id = quote_identifier(id_col)
                    return f"SELECT COUNT(DISTINCT {quoted_id}) as patient_count FROM {quoted_table}"
                else:
                    return f"SELECT COUNT(*) as count FROM {quoted_table}"

        # Finance-specific
        if dataset_type == "finance":
            if 'price' in question_lower or 'stock' in question_lower:
                if quoted_date and quoted_num0:
                    return f"SELECT {quoted_date}, {quoted_num0} FROM {quoted_table} ORDER BY {quoted_date} DESC LIMIT {limit}"

        # DEFAULT: Show meaningful summary
        if quoted_num0 and quoted_cat0:
            return f"SELECT {quoted_cat0}, {agg_func}({quoted_num0}) as total FROM {quoted_table} GROUP BY {quoted_cat0} ORDER BY total DESC LIMIT 20"
        elif quoted_cat0:
            return f"SELECT {quoted_cat0}, COUNT(*) as count FROM {quoted_table} GROUP BY {quoted_cat0} ORDER BY count DESC LIMIT 20"
        elif quoted_num0:
            return f"SELECT {agg_func}({quoted_num0}) as result, COUNT(*) as count FROM {quoted_table}"
        else:
            return f"SELECT * FROM {quoted_table} LIMIT 20"

    async def generate_sql(self, question: str) -> str:
        """Generate SQL from natural language question"""
        return self._generate_sql_local(question)

    async def execute_sql(self, sql: str) -> Dict[str, Any]:
        """Execute SQL on DuckDB and return results with timeout and validation"""
        if not os.path.exists(WAREHOUSE_DB_PATH):
            return {"error": "Warehouse not found. Upload and process data first."}

        # Validate SQL safety
        is_safe, reason = validate_sql(sql)
        if not is_safe:
            return {"error": f"SQL validation failed: {reason}"}

        con = duckdb.connect(WAREHOUSE_DB_PATH, read_only=True)
        con.execute(f"SET timeout={QUERY_TIMEOUT * 1000}")

        try:
            result = con.execute(sql).fetchall()
            columns = [desc[0] for desc in con.description]
            con.close()

            return {
                "columns": columns,
                "data": [dict(zip(columns, row)) for row in result],
                "row_count": len(result)
            }
        except duckdb.Error as e:
            con.close()
            return {"error": str(e)}

    async def process_query(self, question: str) -> Dict[str, Any]:
        """Process a natural language query end-to-end"""
        start_time = time.time()

        # Refresh schema
        self._refresh_schema()

        # Generate SQL
        sql = await self.generate_sql(question)

        # Validate SQL
        is_safe, reason = self._validate_sql(sql)
        if not is_safe:
            return {
                "status": "error",
                "message": f"Generated SQL contains unsafe operations: {reason}",
                "sql": sql
            }

        # Execute SQL with retries
        result = None
        for attempt in range(self.max_retries + 1):
            result = await self.execute_sql(sql)

            if "error" not in result:
                break

            # Try to regenerate SQL on error
            if attempt < self.max_retries:
                table_name = self.schema.get('table_name', 'data_clean')
                quoted_table = quote_identifier(table_name) if validate_identifier(table_name) else '"data_clean"'
                sql = f"SELECT * FROM {quoted_table} LIMIT 20"

        execution_time = time.time() - start_time

        if "error" in result:
            return {
                "status": "error",
                "sql": sql,
                "message": result["error"],
                "execution_time": execution_time
            }

        return {
            "status": "success",
            "question": question,
            "sql": sql,
            "columns": result["columns"],
            "data": result["data"],
            "row_count": result["row_count"],
            "execution_time": round(execution_time, 3),
            "schema": {
                "table": self.schema.get('table_name'),
                "dataset_type": self.schema.get('dataset_type'),
                "suggested_queries": self.schema.get('suggested_queries', [])
            }
        }

    async def get_suggested_queries(self) -> List[str]:
        """Get suggested queries based on current schema"""
        self._refresh_schema()
        return self.schema.get('suggested_queries', [])

    async def get_current_schema(self) -> Dict[str, Any]:
        """Get current detected schema"""
        self._refresh_schema()
        return self.schema
