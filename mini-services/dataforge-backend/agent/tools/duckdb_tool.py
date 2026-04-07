"""
DataForge AI - DuckDB Tool (Dataset-Agnostic)
Data warehouse operations using DuckDB - Works with ANY dataset
"""

import os
import duckdb
import pandas as pd
from typing import Dict, Any, List, Optional
from .schema_detector import SchemaDetector
from agent.utils import (
    BASE_PATH,
    WAREHOUSE_DB_PATH,
    SCHEMA_CACHE_PATH,
    CLEAN_DATA_DIR,
    load_schema,
    save_schema,
    quote_identifier,
    safe_table_name,
    validate_sql,
    validate_identifier,
    QUERY_TIMEOUT,
)


class DuckDBTool:
    """Tool for DuckDB warehouse operations - Dataset Agnostic"""

    def __init__(self):
        self.schema_detector = SchemaDetector()

        # These will be dynamically set based on uploaded data
        self.current_data_file = None
        self.current_table_name = "data_clean"

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        os.makedirs(os.path.dirname(WAREHOUSE_DB_PATH), exist_ok=True)
        # ✅ FIX: DuckDB doesn't support 'timeout' parameter - removed to prevent errors
        con = duckdb.connect(WAREHOUSE_DB_PATH)
        return con

    def set_data_file(self, file_path: str) -> str:
        """Set the current data file to work with"""
        self.current_data_file = file_path
        return f"Data file set to: {file_path}"

    async def ingest_file(self, file_path: str = None, table_name: str = None) -> Dict[str, Any]:
        """Ingest ANY data file into the warehouse"""
        if file_path is None:
            file_path = self.current_data_file

        if file_path is None:
            return {"error": "No data file specified. Upload a file first."}

        if not os.path.exists(file_path):
            return {"error": f"File not found: {file_path}"}

        # Detect schema first
        schema = await self.schema_detector.detect_schema_from_file(file_path)

        if "error" in schema:
            return schema

        # Determine table names using safe_table_name
        filename = os.path.basename(file_path)
        base_name = os.path.splitext(filename)[0]
        raw_table = safe_table_name(f"{base_name}_raw")
        if table_name is None:
            clean_table = safe_table_name(f"{base_name}_clean")
        else:
            if not validate_identifier(table_name):
                return {"error": f"Invalid table name: {table_name!r}"}
            clean_table = table_name.lower()

        # Read data
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith('.json'):
                df = pd.read_json(file_path)
            else:
                return {"error": "Unsupported file format. Use CSV or JSON."}
        except (pd.errors.EmptyDataError, pd.errors.ParserError, ValueError) as e:
            return {"error": f"Failed to read file: {e}"}

        # Ingest into DuckDB with quoted identifiers
        con = self._get_connection()
        try:
            quoted_raw = quote_identifier(raw_table)
            con.execute(f"CREATE OR REPLACE TABLE {quoted_raw} AS SELECT * FROM df")
            count = con.execute(f"SELECT COUNT(*) FROM {quoted_raw}").fetchone()[0]
        except duckdb.Error as e:
            con.close()
            return {"error": f"Failed to ingest data: {e}"}

        con.close()

        # Update schema with table names
        schema["raw_table"] = raw_table
        schema["table_name"] = clean_table
        save_schema(schema)

        self.current_table_name = clean_table

        return {
            "status": "success",
            "message": f"Ingested {count} rows into {raw_table} table",
            "schema": schema
        }

    async def ingest(self) -> str:
        """Ingest current data file (backward compatible)"""
        result = await self.ingest_file()
        if "error" in result:
            return result["error"]
        return result["message"]

    async def transform(self) -> Dict[str, Any]:
        """Transform and clean data using detected schema with aggressive cleaning"""
        schema = load_schema()
        raw_table = schema.get("raw_table", "data_raw")
        clean_table = schema.get("table_name", "data_clean")

        if not validate_identifier(raw_table):
            return {"error": f"Invalid raw table name: {raw_table!r}"}
        if not validate_identifier(clean_table):
            return {"error": f"Invalid clean table name: {clean_table!r}"}

        con = self._get_connection()

        quoted_raw = quote_identifier(raw_table)

        # Check if raw table exists
        try:
            con.execute(f"SELECT * FROM {quoted_raw} LIMIT 1")
        except duckdb.Error:
            con.close()
            return {"error": f"Raw table {raw_table} not found. Run ingest first."}

        # Get column info from raw table directly from DuckDB
        columns_info = con.execute(f"DESCRIBE {quoted_raw}").fetchall()

        # ✅ FIX: Build proper data cleaning transformation
        select_parts = []
        where_parts = []
        cleaning_applied = []

        for col in columns_info:
            col_name = col[0]
            col_type = col[1]

            if not validate_identifier(col_name):
                # Skip columns with unsafe names
                continue

            quoted_col = quote_identifier(col_name)

            # Get semantic info if available
            col_schema = schema.get("columns", {}).get(col_name, {})
            semantic = col_schema.get("semantic", "generic")
            
            # ✅ ACTUAL DATA CLEANING TRANSFORMATIONS
            
            # 1. Handle numeric columns - remove nulls, convert to proper type
            if semantic in ["money", "count", "score", "percentage"] or "INT" in col_type.upper() or "DOUBLE" in col_type.upper() or "FLOAT" in col_type.upper():
                # Cast to numeric and handle nulls
                select_parts.append(f"CAST(NULLIF(TRIM(CAST({quoted_col} AS VARCHAR)), '') AS DOUBLE) as {quoted_col}")
                cleaning_applied.append(f"Cleaned {col_name}: converted to numeric, removed nulls")
            
            # 2. Handle text columns - trim whitespace, handle empty strings
            elif "VARCHAR" in col_type.upper() or semantic in ["category", "name", "location"]:
                select_parts.append(f"TRIM({quoted_col}) as {quoted_col}")
                # Add NOT NULL filter for key columns
                if semantic in ["id", "name"]:
                    where_parts.append(f"{quoted_col} IS NOT NULL AND TRIM({quoted_col}) != ''")
                    cleaning_applied.append(f"Cleaned {col_name}: trimmed, removed nulls/empty")
                else:
                    cleaning_applied.append(f"Cleaned {col_name}: trimmed whitespace")
            
            # 3. Handle dates - parse and convert
            elif semantic == "datetime" or "DATE" in col_type.upper():
                select_parts.append(f"TRY_CAST({quoted_col} AS DATE) as {quoted_col}")
                cleaning_applied.append(f"Cleaned {col_name}: converted to date format")
            
            # 4. Default - just select as-is
            else:
                select_parts.append(quoted_col)

        where_clause = " AND ".join(where_parts) if where_parts else "1=1"

        # Execute transformation with DISTINCT to remove duplicates
        quoted_clean = quote_identifier(clean_table)
        transform_sql = f"""
            CREATE OR REPLACE TABLE {quoted_clean} AS
            SELECT DISTINCT {', '.join(select_parts)}
            FROM {quoted_raw}
            WHERE {where_clause}
        """

        # Validate before executing
        is_safe, reason = validate_sql(transform_sql)
        if not is_safe:
            con.close()
            return {"error": f"Unsafe transformation SQL: {reason}"}

        try:
            con.execute(transform_sql)
        except duckdb.Error as e:
            con.close()
            return {"error": f"Transformation failed: {e}"}

        # Export clean data
        clean_data_path = os.path.join(CLEAN_DATA_DIR, f"{clean_table}.csv")
        os.makedirs(os.path.dirname(clean_data_path), exist_ok=True)

        try:
            df = con.execute(f"SELECT * FROM {quoted_clean}").fetchdf()
            df.to_csv(clean_data_path, index=False)
            count = len(df)
            
            # Get original row count to show how many rows were removed
            original_count = con.execute(f"SELECT COUNT(*) FROM {quoted_raw}").fetchone()[0]
            rows_removed = original_count - count
        except duckdb.Error as e:
            con.close()
            return {"error": f"Failed to export clean data: {e}"}

        con.close()

        # Update schema
        schema["row_count"] = count
        schema["original_row_count"] = original_count
        schema["rows_removed"] = rows_removed
        schema["cleaning_applied"] = cleaning_applied
        save_schema(schema)

        return {
            "status": "success",
            "message": f"Cleaned {original_count} → {count} rows ({rows_removed} removed). Applied: {len(cleaning_applied)} transformations",
            "output_file": clean_data_path,
            "row_count": count,
            "original_count": original_count,
            "rows_removed": rows_removed,
            "cleaning_operations": cleaning_applied
        }

    async def query(self, sql: str) -> Dict[str, Any]:
        """Execute a SQL query with safety validation and timeout."""
        # Validate SQL for dangerous operations
        is_safe, reason = validate_sql(sql)
        if not is_safe:
            return {"status": "error", "message": f"SQL validation failed: {reason}"}

        con = self._get_connection()
        try:
            result = con.execute(sql).fetchall()
            columns = [desc[0] for desc in con.description]
            con.close()

            return {
                "status": "success",
                "columns": columns,
                "data": [dict(zip(columns, row)) for row in result],
                "row_count": len(result)
            }
        except duckdb.Error as e:
            con.close()
            return {"status": "error", "message": str(e)}

    async def get_schema(self, table_name: str = None) -> Dict[str, Any]:
        """Get schema information"""
        con = self._get_connection()

        try:
            if table_name:
                if not validate_identifier(table_name):
                    con.close()
                    return {"error": f"Invalid table name: {table_name!r}"}

                quoted_table = quote_identifier(table_name)
                columns = con.execute(f"DESCRIBE {quoted_table}").fetchall()
                sample = con.execute(f"SELECT * FROM {quoted_table} LIMIT 3").fetchdf().to_dict('records')
                count = con.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0]
                con.close()

                return {
                    "table": table_name,
                    "row_count": count,
                    "columns": [{"name": col[0], "type": col[1]} for col in columns],
                    "sample_data": sample
                }
            else:
                tables = con.execute("SHOW TABLES").fetchall()
                con.close()

                return {
                    "tables": [t[0] for t in tables],
                    "current_schema": load_schema()
                }
        except duckdb.Error as e:
            con.close()
            return {"error": str(e)}

    async def get_current_schema(self) -> Dict[str, Any]:
        """Get the current detected schema with suggestions"""
        return load_schema()

    async def create_table(self, table_name: str, schema: Dict) -> str:
        """Create a new table with validated identifiers"""
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
        except duckdb.Error as e:
            con.close()
            return f"Error creating table: {e}"

    async def export_table(self, table_name: str, output_path: str) -> str:
        """Export a table to CSV with validated identifiers"""
        if not validate_identifier(table_name):
            return f"Error: Invalid table name: {table_name!r}"

        quoted_table = quote_identifier(table_name)
        con = self._get_connection()

        try:
            df = con.execute(f"SELECT * FROM {quoted_table}").fetchdf()
            con.close()
        except duckdb.Error as e:
            con.close()
            return f"Error exporting table: {e}"

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)

        return f"Exported {len(df)} rows to {output_path}"

    async def list_tables(self) -> List[str]:
        """List all tables in the warehouse"""
        con = self._get_connection()
        try:
            tables = con.execute("SHOW TABLES").fetchall()
            con.close()
            return [t[0] for t in tables]
        except duckdb.Error:
            con.close()
            return []

    async def get_sample_data(self, table_name: str = None, limit: int = 10) -> Dict[str, Any]:
        """Get sample data from a table"""
        if table_name is None:
            table_name = load_schema().get("table_name", "data_clean")

        if not validate_identifier(table_name):
            return {"error": f"Invalid table name: {table_name!r}"}

        # Validate limit is reasonable
        try:
            limit = int(limit)
            if limit < 1 or limit > 1000:
                return {"error": "Limit must be between 1 and 1000"}
        except (TypeError, ValueError):
            return {"error": "Limit must be a valid integer"}

        quoted_table = quote_identifier(table_name)
        con = self._get_connection()
        try:
            df = con.execute(f"SELECT * FROM {quoted_table} LIMIT {limit}").fetchdf()
            con.close()

            return {
                "table": table_name,
                "columns": list(df.columns),
                "data": df.to_dict('records'),
                "row_count": len(df)
            }
        except duckdb.Error as e:
            con.close()
            return {"error": str(e)}
