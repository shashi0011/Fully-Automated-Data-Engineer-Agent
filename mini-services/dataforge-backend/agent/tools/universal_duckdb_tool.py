"""
DataForge AI - Universal DuckDB Tool
Works with ANY dataset type using configuration system
"""

import os
import duckdb
import pandas as pd
from typing import Dict, Any, List, Optional
from utils.config import config_manager, DatasetConfig, SchemaConverter


class UniversalDuckDBTool:
    """Universal tool for DuckDB warehouse operations - works with ANY dataset"""
    
    def __init__(self):
        self.base_path = "/home/z/my-project"
        self.warehouse_path = os.path.join(self.base_path, "warehouse/warehouse.duckdb")
        self._current_config: Optional[DatasetConfig] = None
    
    def set_dataset(self, dataset_name: str) -> bool:
        """Set the current dataset configuration"""
        config = config_manager.get_config(dataset_name)
        if config:
            self._current_config = config
            return True
        return False
    
    def get_config(self) -> Optional[DatasetConfig]:
        """Get current dataset configuration"""
        if not self._current_config:
            self._current_config = config_manager.get_current_config()
        return self._current_config
    
    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get a DuckDB connection"""
        os.makedirs(os.path.dirname(self.warehouse_path), exist_ok=True)
        return duckdb.connect(self.warehouse_path)
    
    # =========================================================================
    # SCHEMA DISCOVERY
    # =========================================================================
    
    async def discover_schema(self, csv_path: str) -> Dict[str, Any]:
        """Auto-discover schema from a CSV file"""
        df = pd.read_csv(csv_path)
        
        schema = {
            "file": csv_path,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": []
        }
        
        for col in df.columns:
            col_info = {
                "name": col,
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isna().sum()),
                "null_percentage": round(df[col].isna().sum() / len(df) * 100, 2),
                "unique_count": int(df[col].nunique()),
                "sample_values": df[col].dropna().head(5).tolist()
            }
            
            # Detect semantic type
            if "date" in col.lower() or "time" in col.lower():
                col_info["semantic_type"] = "datetime"
            elif df[col].dtype in ["int64", "float64"]:
                col_info["semantic_type"] = "numeric"
                col_info["min"] = float(df[col].min())
                col_info["max"] = float(df[col].max())
                col_info["mean"] = float(df[col].mean())
            else:
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio < 0.05:
                    col_info["semantic_type"] = "category"
                else:
                    col_info["semantic_type"] = "text"
            
            schema["columns"].append(col_info)
        
        return schema
    
    # =========================================================================
    # INGEST OPERATIONS
    # =========================================================================
    
    async def ingest(self, csv_path: str = None, table_name: str = None) -> str:
        """
        Ingest CSV data into DuckDB warehouse.
        Uses configuration if available, otherwise auto-detects.
        """
        config = self.get_config()
        
        # Determine source file
        source_file = csv_path
        if not source_file and config:
            source_file = os.path.join(self.base_path, config.source_file)
        
        if not source_file or not os.path.exists(source_file):
            return f"Error: Source file not found"
        
        # Determine table name
        raw_table = table_name
        if not raw_table and config:
            raw_table = f"{config.name}_raw"
        if not raw_table:
            raw_table = os.path.basename(source_file).replace(".csv", "_raw")
        
        # Read CSV
        df = pd.read_csv(source_file)
        
        # Connect and create table
        con = self._get_connection()
        
        # Use schema from config if available
        if config:
            # Create table with proper types from config
            con.execute(f"DROP TABLE IF EXISTS {raw_table}")
            con.execute("CREATE TABLE temp_raw AS SELECT * FROM df")
            
            # Apply type casts from configuration
            select_parts = []
            for col in config.columns:
                if col.data_type == "date":
                    select_parts.append(f"CAST(\"{col.name}\" AS DATE) as \"{col.name}\"")
                elif col.data_type == "integer":
                    select_parts.append(f"CAST(\"{col.name}\" AS INTEGER) as \"{col.name}\"")
                elif col.data_type == "float":
                    select_parts.append(f"CAST(\"{col.name}\" AS DOUBLE) as \"{col.name}\"")
                else:
                    select_parts.append(f"\"{col.name}\"")
            
            con.execute(f"CREATE TABLE {raw_table} AS SELECT {', '.join(select_parts)} FROM temp_raw")
            con.execute("DROP TABLE temp_raw")
        else:
            # Auto-create table
            con.execute(f"CREATE OR REPLACE TABLE {raw_table} AS SELECT * FROM df")
        
        # Verify
        count = con.execute(f"SELECT COUNT(*) FROM {raw_table}").fetchone()[0]
        con.close()
        
        return f"Ingested {count} rows into {raw_table} table"
    
    # =========================================================================
    # TRANSFORM OPERATIONS
    # =========================================================================
    
    async def transform(self, source_table: str = None, target_table: str = None) -> str:
        """
        Transform and clean data according to configuration.
        Applies cleaning rules, type casts, and calculated columns.
        """
        config = self.get_config()
        
        if not config:
            return "Error: No dataset configuration loaded. Use set_dataset() first."
        
        # Determine table names
        raw_table = source_table or f"{config.name}_raw"
        clean_table = target_table or f"{config.name}_clean"
        
        con = self._get_connection()
        
        # Check if raw table exists
        try:
            con.execute(f"SELECT * FROM {raw_table} LIMIT 1")
        except:
            # Try to ingest first
            await self.ingest()
        
        # Build transformation SQL
        select_parts = []
        where_parts = []
        
        # Apply column transformations
        for col in config.columns:
            col_select = f'"{col.name}"'
            
            # Apply type casts
            if col.data_type == "date":
                col_select = f'CAST("{col.name}" AS DATE)'
            elif col.data_type == "integer":
                col_select = f'CAST("{col.name}" AS INTEGER)'
            elif col.data_type == "float":
                col_select = f'CAST("{col.name}" AS DOUBLE)'
            
            select_parts.append(f"{col_select} as \"{col.name}\"")
            
            # Apply cleaning rules
            if col.is_required:
                where_parts.append(f'"{col.name}" IS NOT NULL')
        
        # Apply cleaning rules from config
        cleaning = config.cleaning_rules
        if "positive_values" in cleaning:
            for col_name in cleaning["positive_values"]:
                where_parts.append(f'"{col_name}" > 0')
        
        # Add calculated columns
        transforms = config.transformation_rules
        if "calculated_columns" in transforms:
            for col_name, expression in transforms["calculated_columns"].items():
                # Replace column references with quoted names
                expr = expression
                for col in config.columns:
                    expr = expr.replace(col.name, f'"{col.name}"')
                select_parts.append(f"{expr} as \"{col_name}\"")
        
        # Build and execute SQL
        sql = f"SELECT {', '.join(select_parts)} FROM {raw_table}"
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)
        
        con.execute(f"CREATE OR REPLACE TABLE {clean_table} AS {sql}")
        
        # Export to CSV
        clean_path = os.path.join(self.base_path, f"data/clean/{config.name}_clean.csv")
        os.makedirs(os.path.dirname(clean_path), exist_ok=True)
        
        df = con.execute(f"SELECT * FROM {clean_table}").fetchdf()
        df.to_csv(clean_path, index=False)
        
        count = len(df)
        con.close()
        
        return f"Transformed {count} rows, saved to {clean_path}"
    
    # =========================================================================
    # QUERY OPERATIONS
    # =========================================================================
    
    async def query(self, sql: str) -> Dict[str, Any]:
        """Execute a SQL query on the warehouse"""
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
        except Exception as e:
            con.close()
            return {"status": "error", "message": str(e)}
    
    async def execute_pattern(self, pattern_name: str, **kwargs) -> Dict[str, Any]:
        """Execute a named query pattern from configuration"""
        config = self.get_config()
        
        if not config or pattern_name not in config.query_patterns:
            return {"status": "error", "message": f"Pattern '{pattern_name}' not found"}
        
        pattern = config.query_patterns[pattern_name]
        table = f"{config.name}_clean"
        limit = kwargs.get("limit", 10)
        
        sql = pattern.format(table=table, limit=limit)
        
        return await self.query(sql)
    
    # =========================================================================
    # SCHEMA OPERATIONS
    # =========================================================================
    
    async def get_schema(self, table_name: str = None) -> Dict[str, Any]:
        """Get schema information for a table"""
        con = self._get_connection()
        
        if table_name:
            try:
                columns = con.execute(f"DESCRIBE {table_name}").fetchall()
                row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                con.close()
                
                return {
                    "table": table_name,
                    "row_count": row_count,
                    "columns": [{"name": col[0], "type": col[1]} for col in columns]
                }
            except Exception as e:
                con.close()
                return {"error": str(e)}
        else:
            tables = con.execute("SHOW TABLES").fetchall()
            con.close()
            return {"tables": [t[0] for t in tables]}
    
    async def list_tables(self) -> List[Dict[str, Any]]:
        """List all tables with their row counts"""
        con = self._get_connection()
        
        try:
            tables = con.execute("SHOW TABLES").fetchall()
            result = []
            
            for table in tables:
                table_name = table[0]
                try:
                    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    columns = con.execute(f"DESCRIBE {table_name}").fetchall()
                    
                    result.append({
                        "name": table_name,
                        "row_count": row_count,
                        "column_count": len(columns),
                        "columns": [col[0] for col in columns]
                    })
                except:
                    pass
            
            con.close()
            return result
        except Exception as e:
            con.close()
            return []
    
    # =========================================================================
    # EXPORT OPERATIONS
    # =========================================================================
    
    async def export_table(self, table_name: str, output_path: str) -> str:
        """Export a table to CSV"""
        con = self._get_connection()
        
        try:
            df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
            con.close()
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_csv(output_path, index=False)
            
            return f"Exported {len(df)} rows to {output_path}"
        except Exception as e:
            con.close()
            return f"Error: {str(e)}"
    
    # =========================================================================
    # DATASET MANAGEMENT
    # =========================================================================
    
    async def load_dataset(self, csv_path: str, dataset_name: str = None) -> Dict[str, Any]:
        """
        Load a new dataset into the warehouse.
        Auto-detects schema if no configuration exists.
        """
        # Try to get existing config
        if dataset_name and dataset_name in config_manager.list_datasets():
            self.set_dataset(dataset_name)
            result = await self.ingest()
            return {"status": "success", "message": result, "config": "existing"}
        
        # Auto-detect schema
        schema = await self.discover_schema(csv_path)
        
        # Create auto-config
        auto_config = config_manager.auto_detect_schema(csv_path, dataset_name)
        config_manager.add_custom_config(auto_config)
        self._current_config = auto_config
        
        # Ingest the data
        result = await self.ingest(csv_path)
        
        return {
            "status": "success",
            "message": result,
            "config": "auto-detected",
            "schema": schema,
            "dataset_name": auto_config.name
        }


# Create global instance
universal_duckdb_tool = UniversalDuckDBTool()
