"""
DataForge AI - Pipeline Generator (Dataset-Agnostic)
Generates executable pipeline code dynamically for ANY dataset.
Each dataset gets its own pipeline file.
"""

import os
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

from agent.utils import (
    BASE_PATH,
    SCHEMA_CACHE_PATH,
    WAREHOUSE_DB_PATH,
    PIPELINES_DIR,
    REPORTS_DIR,
    load_schema,
)


class PipelineGenerator:
    """Generates Python pipeline code dynamically - Dataset Agnostic"""

    def __init__(self):
        self.base_path = BASE_PATH

    def _load_schema(self) -> Dict[str, Any]:
        return load_schema() or {"table_name": "data_clean", "columns": {}, "dataset_type": "generic"}

    @staticmethod
    def _get_pipeline_filename(schema: Dict) -> str:
        """Generate a dataset-specific pipeline filename."""
        dataset_type = schema.get("dataset_type", "generic")
        table_name = schema.get("table_name", "data_clean")
        # e.g. "sales_pipeline.py" or "medical_records_pipeline.py"
        base = table_name.replace("_clean", "")
        return f"{base}_pipeline.py"

    @staticmethod
    def _get_pipeline_path(schema: Dict) -> str:
        filename = PipelineGenerator._get_pipeline_filename(schema)
        return os.path.join(PIPELINES_DIR, filename)

    def _get_ingest_method(self, schema: Dict) -> str:
        source_file = schema.get('source_file', 'data/raw/data.csv')
        raw_table = schema.get('raw_table', 'data_raw')

        return f'''def ingest(self):
        """Ingest raw data into DuckDB warehouse"""
        self.log("[Ingest] Reading raw data...")
        
        if not os.path.exists(self.raw_data_path):
            self.log(f"[Ingest] Error: Raw data file not at {{self.raw_data_path}}")
            return False
        
        df = pd.read_csv(self.raw_data_path)
        self.log(f"[Ingest] Loaded {{len(df)}} rows")
        
        os.makedirs(os.path.dirname(self.warehouse_path), exist_ok=True)
        
        con = duckdb.connect(self.warehouse_path)
        con.execute("CREATE OR REPLACE TABLE {raw_table} AS SELECT * FROM df")
        
        count = con.execute("SELECT COUNT(*) FROM {raw_table}").fetchone()[0]
        con.close()
        
        self.log(f"[Ingest] Data ingested to warehouse ({{count}} rows)")
        return True'''

    def _get_transform_method(self, schema: Dict) -> str:
        raw_table = schema.get('raw_table', 'data_raw')
        clean_table = schema.get('table_name', 'data_clean')
        columns = schema.get('columns', {})

        select_parts = []
        where_parts = []

        for col_name, col_info in columns.items():
            semantic = col_info.get('semantic', 'generic')
            dtype = col_info.get('type', '').lower()

            if 'int' in dtype or semantic == 'count':
                select_parts.append(f"CAST({col_name} AS INTEGER) as {col_name}")
            elif 'double' in dtype or 'float' in dtype or semantic in ['money', 'percentage', 'score']:
                select_parts.append(f"CAST({col_name} AS DOUBLE) as {col_name}")
            elif semantic == 'datetime' or 'date' in dtype:
                select_parts.append(f"TRY_CAST({col_name} AS DATE) as {col_name}")
            else:
                select_parts.append(col_name)

            if semantic in ['id', 'name']:
                where_parts.append(f"{col_name} IS NOT NULL")

        where_clause = " AND ".join(where_parts) if where_parts else "1=1"
        select_clause = ",\n                ".join(select_parts)

        return f'''def transform(self):
        """Transform and clean data"""
        self.log("[Transform] Cleaning data...")
        
        con = duckdb.connect(self.warehouse_path)
        
        con.execute("""
            CREATE OR REPLACE TABLE {clean_table} AS
            SELECT 
                {select_clause}
            FROM {raw_table}
            WHERE {where_clause}
        """)
        
        os.makedirs(os.path.dirname(self.clean_data_path), exist_ok=True)
        df = con.execute("SELECT * FROM {clean_table}").fetchdf()
        df.to_csv(self.clean_data_path, index=False)
        
        count = len(df)
        con.close()
        
        self.log(f"[Transform] Cleaned {{count}} rows saved")
        return True'''

    def _get_report_method(self, schema: Dict) -> str:
        clean_table = schema.get('table_name', 'data_clean')
        columns = schema.get('columns', {})

        group_cols = []
        numeric_cols = []

        for col_name, col_info in columns.items():
            semantic = col_info.get('semantic', 'generic')
            dtype = col_info.get('type', '').lower()
            if semantic in ['category', 'location', 'name']:
                group_cols.append(col_name)
            elif semantic in ['money', 'count', 'score'] or 'int' in dtype or 'float' in dtype:
                numeric_cols.append(col_name)

        if group_cols and numeric_cols:
            group_by = group_cols[0]
            metric = numeric_cols[0]
            report_sql = f"""SELECT 
                {group_by},
                COUNT(*) as record_count,
                SUM({metric}) as total_{metric},
                ROUND(AVG({metric}), 2) as avg_{metric}
            FROM {clean_table}
            GROUP BY {group_by}
            ORDER BY total_{metric} DESC"""
        elif group_cols:
            group_by = group_cols[0]
            report_sql = f"""SELECT 
                {group_by},
                COUNT(*) as record_count
            FROM {clean_table}
            GROUP BY {group_by}
            ORDER BY record_count DESC"""
        else:
            report_sql = f"""SELECT 
                COUNT(*) as total_records
            FROM {clean_table}"""

        return f'''def generate_report(self):
        """Generate summary report"""
        self.log("[Report] Generating report...")
        
        con = duckdb.connect(self.warehouse_path)
        report_df = con.execute("""
            {report_sql}
        """).fetchdf()
        
        os.makedirs(os.path.dirname(self.report_path), exist_ok=True)
        report_df.to_csv(self.report_path, index=False)
        
        count = len(report_df)
        con.close()
        
        self.log(f"[Report] Generated report with {{count}} rows")
        return True'''

    def generate(self, operations: List[str], schema: Dict = None) -> str:
        """Generate pipeline code for given operations and schema."""
        if schema is None:
            schema = self._load_schema()

        source_file = schema.get('source_file', 'data/raw/data.csv')
        clean_table = schema.get('table_name', 'data_clean')
        raw_table = schema.get('raw_table', 'data_raw')
        dataset_type = schema.get('dataset_type', 'generic')

        rel_source = source_file
        if rel_source.startswith(self.base_path):
            rel_source = rel_source[len(self.base_path):].lstrip("/").lstrip("\\")

        pipeline_filename = self._get_pipeline_filename(schema)

        template = f'''"""
DataForge AI - Generated Pipeline
Auto-generated by DataForge Agent
Generated at: {datetime.now().isoformat()}
Operations: {", ".join(operations)}
Dataset Type: {dataset_type}
"""

import duckdb
import pandas as pd
from datetime import datetime
import os

class DataPipeline:
    """Auto-generated Data Pipeline for {dataset_type} dataset"""

    def __init__(self):
        self.base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        self.warehouse_path = os.path.join(self.base_path, "warehouse", "warehouse.duckdb")
        self.raw_data_path = os.path.join(self.base_path, "{rel_source}")
        self.clean_data_path = os.path.join(self.base_path, "data", "clean", "{clean_table}.csv")
        self.report_path = os.path.join(self.base_path, "reports", "{clean_table}_report.csv")
        self.logs = []

    def log(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.logs.append(f"[{{timestamp}}] {{message}}")
        print(message)
'''

        methods = []
        execution_steps = []

        if "ingest" in operations:
            methods.append(self._get_ingest_method(schema))
            execution_steps.append('''
        self.log("Step 1: Ingesting data...")
        results["ingest"] = self.ingest()
        if results["ingest"]:
            files.append("warehouse/warehouse.duckdb")''')

        if "transform" in operations:
            methods.append(self._get_transform_method(schema))
            step_num = len([s for s in execution_steps if "Step" in s]) + 1
            execution_steps.append(f'''
        self.log("Step {step_num}: Transforming data...")
        results["transform"] = self.transform()
        if results["transform"]:
            files.append("data/clean/{clean_table}.csv")''')

        if "report" in operations:
            methods.append(self._get_report_method(schema))
            step_num = len([s for s in execution_steps if "Step" in s]) + 1
            execution_steps.append(f'''
        self.log("Step {step_num}: Generating report...")
        results["report"] = self.generate_report()
        if results["report"]:
            files.append("reports/{clean_table}_report.csv")''')

        run_method = f'''
    def run(self):
        """Execute full pipeline"""
        start_time = datetime.now()
        self.log("=" * 50)
        self.log("DataForge AI Pipeline Execution")
        self.log(f"Dataset: {dataset_type}")
        self.log(f"Started: {{start_time}}")
        self.log("=" * 50)
        
        results = {{}}
        files = []
        
        {" ".join(execution_steps)}
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        self.log("=" * 50)
        self.log(f"Pipeline completed in {{duration:.2f}} seconds")
        self.log("=" * 50)
        
        return {{
            "status": "success",
            "duration": duration,
            "results": results,
            "files": files,
            "logs": self.logs
        }}


if __name__ == "__main__":
    pipeline = DataPipeline()
    result = pipeline.run()
    print(result)
'''

        code = template + "\n    " + "\n    ".join(methods) + run_method

        # Write to dataset-specific file
        pipeline_path = self._get_pipeline_path(schema)
        os.makedirs(os.path.dirname(pipeline_path), exist_ok=True)
        with open(pipeline_path, 'w') as f:
            f.write(code)

        return code

    async def generate_from_file(self, file_path: str, operations: List[str] = None) -> Dict[str, Any]:
        from agent.tools.schema_detector import SchemaDetector

        detector = SchemaDetector()
        schema = await detector.detect_schema_from_file(file_path)

        if "error" in schema:
            return {"error": schema["error"]}

        if operations is None:
            operations = ["ingest", "transform", "report"]

        code = self.generate(operations, schema)

        return {
            "status": "success",
            "schema": schema,
            "pipeline_code": code,
            "output_file": self._get_pipeline_path(schema),
        }
