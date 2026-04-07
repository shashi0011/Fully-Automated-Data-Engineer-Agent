"""
DataForge AI - LangGraph Orchestration
Multi-step agent workflow with conditional routing.

KEY BEHAVIOR:
  - ingest: ONLY saves file + creates raw table in DuckDB. NO transform, NO pipeline.
  - transform / clean: Runs ONLY the transform step (raw → clean table + export CSV)
  - query: Runs ONLY NL→SQL on existing clean table
  - analyze: Runs ONLY the analysis (LLM or heuristic) on existing data
  - generate_dbt: Runs ONLY dbt model generation
  - pipeline: Runs the FULL pipeline (ingest → transform → report → pipeline.py)
  - report: Runs ONLY report generation
  - schema: Shows current schema details
  - help: Lists commands

Graph topology:
    START → classify_intent → load_schema → [conditional routing] → action_node → END
"""

import os
import glob
from typing import Dict, Any, List, Optional
from typing_extensions import TypedDict
from datetime import datetime

from langgraph.graph import StateGraph, END

try:
    from langgraph.graph import START
except ImportError:
    START = "__start__"

from agent.utils import (
    load_schema,
    save_schema,
    BASE_PATH,
    RAW_DATA_DIR,
    PIPELINES_DIR,
    REPORTS_DIR,
    DBT_DIR,
    format_result,
    validate_identifier,
    validate_sql,
)


class AgentState(TypedDict):
    """Shared state across all graph nodes."""
    command: str
    session_id: str
    intent: str
    parsed_params: Dict[str, Any]
    schema: Dict[str, Any]
    dataset_type: str
    current_file: str
    current_table: str
    analysis_result: Dict[str, Any]
    sql_result: Dict[str, Any]
    dbt_result: Dict[str, Any]
    pipeline_steps: List[Dict[str, Any]]
    pipeline_logs: List[str]
    files_generated: List[str]
    status: str
    error_message: Optional[str]
    duration: float
    timestamp: str
    messages: list


# === Keyword → Intent map ===

_INTENT_KEYWORDS: Dict[str, List[str]] = {
    "ingest": ["upload", "ingest", "load", "import"],
    "analyze": ["analyze", "analysis", "inspect", "examine", "what is", "quality", "data quality"],
    "query": [
        "query", "show", "select", "find", "top", "average", "sum",
        "count", "how many", "trend", "total", "best", "worst",
        "compare", "by region", "by product", "monthly", "yearly",
    ],
    "generate_dbt": ["dbt", "generate dbt", "create models", "transform models"],
    "transform": ["clean", "transform", "process data", "normalize", "wash", "preprocess", "clean data", "clean the data", "transform data", "normalize data"],
    "pipeline": ["pipeline", "etl", "process", "run pipeline", "build pipeline", "full pipeline", "run all", "execute pipeline"],
    "report": ["report", "summary", "export", "download", "generate report", "create report"],
    "schema": ["schema", "columns", "structure", "describe"],
    "help": ["help", "what can you do", "commands", "list", "options"],
}


# === Graph Nodes ===


async def classify_intent(state: AgentState) -> dict:
    """Classify the user command into an intent using keyword matching."""
    command = state.get("command", "").lower()

    for intent, keywords in _INTENT_KEYWORDS.items():
        if any(kw in command for kw in keywords):
            return {"intent": intent, "parsed_params": {"raw_command": state.get("command", "")}}

    return {"intent": "help", "parsed_params": {"raw_command": state.get("command", "")}}


async def load_current_schema(state: AgentState) -> dict:
    """Load the current schema from the shared cache."""
    schema = load_schema()
    dataset_type = schema.get("dataset_type", "unknown")
    table = schema.get("table_name", schema.get("table", "none"))
    return {
        "schema": schema,
        "dataset_type": dataset_type,
        "current_table": table,
    }


async def ingest_data(state: AgentState) -> dict:
    """Ingest the most recent uploaded file into DuckDB warehouse.
    
    ONLY creates a raw table. Does NOT transform or auto-clean.
    """
    from agent.tools.duckdb_tool import DuckDBTool

    try:
        raw_files = sorted(
            glob.glob(os.path.join(RAW_DATA_DIR, "*.csv"))
            + glob.glob(os.path.join(RAW_DATA_DIR, "*.json")),
            key=os.path.getmtime,
            reverse=True,
        )

        if not raw_files:
            return {
                "status": "error",
                "error_message": "No files found in data/raw/. Upload a dataset first.",
            }

        file_path = raw_files[0]
        filename = os.path.basename(file_path)

        duckdb = DuckDBTool()
        result = await duckdb.ingest_file(file_path)

        if "error" in result:
            return {"status": "error", "error_message": result["error"]}

        schema = load_schema()

        logs = [
            f"[Ingest] Ingested {filename}",
            f"[Ingest] Dataset type: {schema.get('dataset_type', 'unknown')}",
            f"[Ingest] Table: {schema.get('raw_table', 'unknown')} ({schema.get('row_count', 0)} rows)",
            f"[Ingest] Columns: {schema.get('column_count', 0)}",
            "[Ingest] READY. Use 'clean' or 'transform' to prepare data.",
        ]

        return {
            "schema": schema,
            "dataset_type": schema.get("dataset_type", "unknown"),
            "current_file": filename,
            "current_table": schema.get("table_name", "unknown"),
            "status": "success",
            "error_message": None,
            "files_generated": ["warehouse/warehouse.duckdb"],
            "pipeline_logs": logs,
        }

    except Exception as exc:
        return {
            "status": "error",
            "error_message": f"Ingestion failed: {exc}",
            "pipeline_logs": [f"ERROR during ingest: {exc}"],
        }


async def run_transform(state: AgentState) -> dict:
    """Transform and clean data (raw → clean table + export CSV).
    Auto-ingests if raw table doesn't exist.
    """
    from agent.tools.duckdb_tool import DuckDBTool

    schema = state.get("schema") or load_schema()

    if not schema or not schema.get("columns"):
        return {
            "status": "error",
            "error_message": "No schema loaded. Upload a dataset first.",
            "pipeline_logs": ["ERROR: No schema found — upload data first"],
        }

    try:
        duckdb = DuckDBTool()

        # Auto-ingest if raw table doesn't exist
        raw_table = schema.get("raw_table", "")
        if not raw_table:
            logs_ingest = ["[Transform] No raw table found — auto-ingesting..."]
            import glob as _glob
            raw_files = sorted(
                _glob.glob(os.path.join(RAW_DATA_DIR, "*.csv"))
                + _glob.glob(os.path.join(RAW_DATA_DIR, "*.json")),
                key=os.path.getmtime, reverse=True,
            )
            if not raw_files:
                return {
                    "status": "error",
                    "error_message": "No files found in data/raw/. Upload a dataset first.",
                    "pipeline_logs": ["ERROR: No raw files found to ingest"],
                }
            ingest_result = await duckdb.ingest_file(raw_files[0])
            if "error" in ingest_result:
                return {
                    "status": "error",
                    "error_message": f"Auto-ingest failed: {ingest_result['error']}",
                    "pipeline_logs": [f"ERROR: {ingest_result['error']}"],
                }
            schema = load_schema()
            logs_ingest.append(f"[Transform] Auto-ingested {os.path.basename(raw_files[0])}")
        else:
            logs_ingest = []

        transform_result = await duckdb.transform()

        if "error" in transform_result:
            return {
                "status": "error",
                "error_message": transform_result["error"],
                "pipeline_logs": logs_ingest + [f"ERROR: {transform_result['error']}"],
            }

        files = ["warehouse/warehouse.duckdb"]
        if "output_file" in transform_result:
            files.append(transform_result["output_file"])

        logs = logs_ingest + [
            f"[Transform] {transform_result.get('message', 'Completed')}",
            f"[Transform] Clean data exported to {transform_result.get('output_file', 'N/A')}",
        ]

        return {
            "status": "success",
            "schema": schema,
            "files_generated": files,
            "pipeline_logs": logs,
        }

    except Exception as exc:
        return {
            "status": "error",
            "error_message": f"Transform failed: {exc}",
            "pipeline_logs": [f"ERROR during transform: {exc}"],
        }


async def run_analysis(state: AgentState) -> dict:
    """Run analysis on the loaded dataset (LLM or heuristic fallback)."""
    from agent.tools.llm_agent import LLMAgent
    from agent.tools.duckdb_tool import DuckDBTool

    try:
        llm = LLMAgent()
        schema = state.get("schema") or load_schema()
        session_id = state.get("session_id", "default")

        # Get sample data from clean table (if it exists)
        sample_data = []
        clean_table = schema.get("table_name", "data_clean")
        if schema.get("columns"):
            try:
                duckdb = DuckDBTool()
                sample_result = await duckdb.get_sample_data(clean_table, limit=50)
                if "data" in sample_result:
                    sample_data = sample_result["data"]
            except Exception:
                pass

        result = await llm.analyze_dataset(
            schema_info=schema,
            sample_data=sample_data,
            session_id=session_id,
        )

        return {
            "analysis_result": result,
            "status": "success",
        }

    except Exception as exc:
        return {
            "status": "error",
            "error_message": f"Analysis failed: {exc}",
            "pipeline_logs": [f"ERROR during analysis: {exc}"],
        }


async def run_query(state: AgentState) -> dict:
    """Generate SQL from natural language and execute it."""
    from agent.tools.llm_agent import LLMAgent
    from agent.tools.duckdb_tool import DuckDBTool
    from agent.tools.schema_detector import SchemaDetector

    try:
        llm = LLMAgent()
        duckdb = DuckDBTool()
        schema = state.get("schema") or load_schema()
        command = state.get("command", "")
        session_id = state.get("session_id", "default")

        # Generate SQL (LLM or fallback)
        sql_result = await llm.generate_sql(
            question=command,
            schema_info=schema,
            session_id=session_id,
        )

        sql = sql_result.get("sql", "")
        if sql:
            is_safe, reason = validate_sql(sql)
            if is_safe:
                exec_result = await duckdb.query(sql)
                sql_result["execution_result"] = exec_result
                if "error" in exec_result:
                    sql_result["execution_error"] = exec_result["error"]
            else:
                sql_result["execution_error"] = f"SQL blocked: {reason}"
        else:
            # Try pattern-based fallback from SchemaDetector
            detector = SchemaDetector()
            sql = detector.generate_sql_suggestions(command, schema)
            sql_result["sql"] = sql
            sql_result["explanation"] = "Pattern-based SQL"
            sql_result["generated_by"] = "pattern"
            is_safe, reason = validate_sql(sql)
            if is_safe:
                exec_result = await duckdb.query(sql)
                sql_result["execution_result"] = exec_result

        return {"sql_result": sql_result, "status": "success"}

    except Exception as exc:
        return {
            "status": "error",
            "error_message": f"Query failed: {exc}",
            "pipeline_logs": [f"ERROR during query: {exc}"],
        }


async def generate_dbt(state: AgentState) -> dict:
    """Generate dbt transformation models AND write them to disk."""
    from agent.tools.llm_agent import LLMAgent

    try:
        llm = LLMAgent()
        schema = state.get("schema") or load_schema()
        session_id = state.get("session_id", "default")

        result = await llm.generate_dbt_models(
            schema_info=schema,
            session_id=session_id,
        )

        # Write models to disk (same as /llm/generate-dbt endpoint)
        saved_files = []
        for model in result.get("models", []):
            model_path = os.path.join(DBT_DIR, model["path"])
            os.makedirs(os.path.dirname(model_path), exist_ok=True)
            with open(model_path, 'w') as f:
                f.write(model["content"])
            saved_files.append(model["path"])

        if result.get("schema_yml"):
            schema_path = os.path.join(DBT_DIR, "models", "schema.yml")
            os.makedirs(os.path.dirname(schema_path), exist_ok=True)
            with open(schema_path, 'w') as f:
                f.write(result["schema_yml"])
            saved_files.append("models/schema.yml")

        files = saved_files if saved_files else [m.get("path", "") for m in result.get("models", []) if m.get("path")]

        return {
            "dbt_result": result,
            "status": "success",
            "files_generated": files,
            "pipeline_logs": [f"[dbt] Generated {len(saved_files)} model files"] if saved_files else [],
        }

    except Exception as exc:
        return {
            "status": "error",
            "error_message": f"dbt generation failed: {exc}",
            "pipeline_logs": [f"ERROR during dbt generation: {exc}"],
        }


async def run_pipeline(state: AgentState) -> dict:
    """Execute the FULL data pipeline: ingest → transform → report → pipeline.py.
    Each step is checked for errors. Stops on critical failure.
    """
    from agent.pipeline_generator import PipelineGenerator
    from agent.tools.duckdb_tool import DuckDBTool
    from agent.tools.report_tool import ReportTool

    schema = state.get("schema") or load_schema()

    if not schema or not schema.get("columns"):
        return {
            "status": "error",
            "error_message": "No schema loaded. Upload a dataset first.",
            "pipeline_logs": ["ERROR: No schema found — upload data first"],
        }

    try:
        generator = PipelineGenerator()
        duckdb = DuckDBTool()
        reporter = ReportTool()

        logs: List[str] = []
        files: List[str] = []

        # --- Step 1: Ingest ---
        raw_files = sorted(
            glob.glob(os.path.join(RAW_DATA_DIR, "*.csv"))
            + glob.glob(os.path.join(RAW_DATA_DIR, "*.json")),
            key=os.path.getmtime,
            reverse=True,
        )
        if raw_files:
            filename = os.path.basename(raw_files[0])
            ingest_result = await duckdb.ingest_file(raw_files[0])
            if "error" in ingest_result:
                logs.append(f"[Ingest] ERROR: {ingest_result['error']}")
                # Don't stop — try to continue with existing warehouse data
            else:
                logs.append(f"[Ingest] Loaded {filename} into warehouse")
                files.append("warehouse/warehouse.duckdb")
                schema = load_schema()  # refresh after ingest
        else:
            logs.append("[Ingest] No raw CSV/JSON found, using existing warehouse data")

        # --- Step 2: Transform ---
        logs.append("[Transform] Starting data cleaning...")
        transform_result = await duckdb.transform()
        if "error" in transform_result:
            logs.append(f"[Transform] ERROR: {transform_result['error']}")
            # Critical — without clean data, report will also fail
            return {
                "status": "error",
                "error_message": f"Transform failed: {transform_result['error']}",
                "pipeline_logs": logs,
                "files_generated": files,
            }
        logs.append(f"[Transform] {transform_result.get('message', 'Done')}")
        if "output_file" in transform_result:
            files.append(transform_result["output_file"])

        # --- Step 3: Generate pipeline.py ---
        logs.append("[Pipeline] Generating pipeline code...")
        try:
            operations = ["ingest", "transform", "report"]
            pipeline_code = generator.generate(operations, schema)
            pipeline_filename = generator._get_pipeline_filename(schema)
            pipeline_path = generator._get_pipeline_path(schema)
            
            # Verify file was actually written
            if os.path.exists(pipeline_path):
                files.append(f"pipelines/{pipeline_filename}")
                logs.append(f"[Pipeline] Written to pipelines/{pipeline_filename}")
                
                # ✅ FIX: Actually execute the generated pipeline
                logs.append(f"[Pipeline] Executing {pipeline_filename}...")
                import subprocess
                import sys
                try:
                    # Execute pipeline with proper Python interpreter
                    result = subprocess.run(
                        [sys.executable, pipeline_path],
                        capture_output=True,
                        text=True,
                        timeout=120,  # 2 minute timeout
                        cwd=BASE_PATH
                    )
                    
                    if result.returncode == 0:
                        logs.append(f"[Pipeline] ✓ Execution completed successfully")
                        # Pipeline execution logs
                        if result.stdout:
                            for line in result.stdout.strip().split('\n')[:10]:  # First 10 lines
                                if line.strip():
                                    logs.append(f"  {line.strip()}")
                    else:
                        logs.append(f"[Pipeline] ✗ Execution failed with code {result.returncode}")
                        if result.stderr:
                            error_lines = result.stderr.strip().split('\n')[:5]
                            for line in error_lines:
                                if line.strip():
                                    logs.append(f"  ERROR: {line.strip()}")
                except subprocess.TimeoutExpired:
                    logs.append(f"[Pipeline] ⚠ Execution timeout (>120s) - pipeline may still be running")
                except Exception as exec_error:
                    logs.append(f"[Pipeline] ✗ Execution error: {exec_error}")
            else:
                logs.append(f"[Pipeline] WARNING: Pipeline file not written to disk")
        except Exception as e:
            logs.append(f"[Pipeline] WARNING: Code generation failed: {e}")

        # --- Step 4: Generate report ---
        logs.append("[Report] Generating summary report...")
        schema_for_report = load_schema()
        report_msg = await reporter.generate(schema=schema_for_report)
        if report_msg.startswith("Error"):
            logs.append(f"[Report] ERROR: {report_msg}")
        else:
            logs.append(f"[Report] {report_msg}")
            report_filename = reporter._get_report_filename(schema_for_report)
            report_path = reporter._get_report_path(schema_for_report)
            if os.path.exists(report_path):
                files.append(f"reports/{report_filename}")
            else:
                logs.append(f"[Report] WARNING: Report file not found on disk")

        pipeline_steps = [
            {"name": "ingest", "operation": "ingest", "description": "Load raw data into DuckDB warehouse"},
            {"name": "transform", "operation": "transform", "description": "Clean and transform data"},
            {"name": "pipeline", "operation": "generate", "description": "Generate pipeline code"},
            {"name": "report", "operation": "report", "description": "Generate summary report"},
        ]

        return {
            "pipeline_steps": pipeline_steps,
            "pipeline_logs": logs,
            "files_generated": files,
            "status": "success",
        }

    except Exception as exc:
        return {
            "status": "error",
            "error_message": f"Pipeline execution failed: {exc}",
            "pipeline_logs": (state.get("pipeline_logs") or []) + [f"ERROR: {exc}"],
        }


async def generate_report(state: AgentState) -> dict:
    """Generate a data report from the current warehouse.
    Auto-transforms if clean table doesn't exist.
    """
    from agent.tools.duckdb_tool import DuckDBTool
    from agent.tools.report_tool import ReportTool

    try:
        schema = state.get("schema") or load_schema()

        # Auto-transform if clean table doesn't exist
        clean_table = schema.get("table_name", "")
        if not clean_table:
            logs_auto = ["[Report] No schema found — upload data first"]
            return {
                "status": "error",
                "error_message": "No schema loaded. Upload a dataset first.",
                "pipeline_logs": logs_auto,
            }

        # Check if clean table exists by trying transform first
        duckdb = DuckDBTool()
        raw_table = schema.get("raw_table", "")
        if raw_table:
            try:
                # Quick check: see if clean table exists in warehouse
                from agent.utils import WAREHOUSE_DB_PATH, validate_identifier, quote_identifier
                import duckdb as _duckdb
                if os.path.exists(WAREHOUSE_DB_PATH) and validate_identifier(clean_table):
                    con = _duckdb.connect(WAREHOUSE_DB_PATH, read_only=True)
                    con.execute(f"SELECT 1 FROM {quote_identifier(clean_table)} LIMIT 1")
                    con.close()
                else:
                    raise _duckdb.Error("no warehouse or invalid table")
            except Exception:
                # Clean table doesn't exist — auto-transform
                logs_auto = ["[Report] Clean table not found — auto-transforming..."]
                transform_result = await duckdb.transform()
                if "error" in transform_result:
                    logs_auto.append(f"[Report] Auto-transform failed: {transform_result['error']}")
                    return {
                        "status": "error",
                        "error_message": f"Cannot generate report: {transform_result['error']}",
                        "pipeline_logs": logs_auto,
                    }
                logs_auto.append(f"[Report] Auto-transform: {transform_result.get('message', 'Done')}")
                schema = load_schema()
        else:
            logs_auto = []

        reporter = ReportTool()
        report_msg = await reporter.generate(schema=schema)

        if report_msg.startswith("Error"):
            return {
                "status": "error",
                "error_message": report_msg,
                "pipeline_logs": (logs_auto if 'logs_auto' in dir() else []) + [f"[Report] {report_msg}"],
            }

        report_filename = reporter._get_report_filename(schema)
        report_path = reporter._get_report_path(schema)

        output_files = []
        if os.path.exists(report_path):
            output_files.append(f"reports/{report_filename}")

        return {
            "status": "success",
            "files_generated": output_files,
            "pipeline_logs": (logs_auto if 'logs_auto' in dir() else []) + [f"[Report] {report_msg}"],
        }

    except Exception as exc:
        return {
            "status": "error",
            "error_message": f"Report generation failed: {exc}",
            "pipeline_logs": [f"ERROR: {exc}"],
        }


async def show_schema(state: AgentState) -> dict:
    """Return the current schema information."""
    schema = state.get("schema") or load_schema()

    if not schema or not schema.get("columns"):
        return {
            "status": "error",
            "error_message": "No schema loaded. Upload a dataset first.",
        }

    columns_info = []
    for col_name, col_info in schema.get("columns", {}).items():
        columns_info.append({
            "name": col_name,
            "type": col_info.get("type", "unknown"),
            "semantic": col_info.get("semantic", "unknown"),
            "nullable": col_info.get("nullable", False),
            "unique_count": col_info.get("unique_count", 0),
            "null_count": col_info.get("null_count", 0),
            "null_pct": col_info.get("null_pct", 0),
            "sample_values": col_info.get("sample_values", []),
            "business_meaning": col_info.get("business_meaning", ""),
        })

    return {
        "status": "success",
        "analysis_result": {
            "schema_info": {
                "dataset_type": schema.get("dataset_type", "unknown"),
                "dataset_description": schema.get("dataset_description", ""),
                "table_name": schema.get("table_name", "unknown"),
                "row_count": schema.get("row_count", 0),
                "column_count": schema.get("column_count", 0),
                "columns": columns_info,
                "column_summary": schema.get("column_summary", {}),
                "suggested_queries": schema.get("suggested_queries", []),
                "detection_method": schema.get("detection_method", "heuristic"),
            }
        },
    }


async def handle_error(state: AgentState) -> dict:
    """Error handler node."""
    error = state.get("error_message", "Unknown error")
    return {
        "status": "error",
        "error_message": error,
        "pipeline_logs": state.get("pipeline_logs", []) + [f"ERROR: {error}"],
    }


async def show_help(state: AgentState) -> dict:
    """Return help information."""
    return {
        "status": "success",
        "analysis_result": {
            "help": {
                "available_intents": list(_INTENT_KEYWORDS.keys()),
                "examples": {
                    "ingest": "Upload a CSV file",
                    "clean / transform": "Clean and transform the data",
                    "analyze": "Analyze the dataset with AI",
                    "query": "show top 10 by revenue",
                    "generate_dbt": "Generate dbt transformation models",
                    "pipeline": "Run full data pipeline (ingest → transform → report)",
                    "report": "Generate summary report",
                    "schema": "Describe schema columns",
                    "help": "Show this help message",
                },
            }
        },
    }


# === Conditional Router ===


def route_by_intent(state: AgentState) -> str:
    """Route to the appropriate action node based on classified intent."""
    intent = state.get("intent", "help")
    schema = state.get("schema", {})
    has_schema = bool(schema and schema.get("columns"))

    if intent == "ingest":
        return "ingest"
    elif intent == "transform":
        return "transform" if has_schema else "error"
    elif intent == "pipeline":
        return "pipeline"
    elif intent == "help":
        return "help"
    elif intent == "schema":
        return "schema_info"
    elif intent in ("analyze", "query", "generate_dbt", "report"):
        return intent if has_schema else "error"
    else:
        return "help"


# === Graph Builder ===


def build_graph() -> StateGraph:
    graph = StateGraph(AgentState)

    graph.add_node("classify_intent", classify_intent)
    graph.add_node("load_schema", load_current_schema)
    graph.add_node("ingest", ingest_data)
    graph.add_node("transform", run_transform)
    graph.add_node("analyze", run_analysis)
    graph.add_node("query", run_query)
    graph.add_node("generate_dbt", generate_dbt)
    graph.add_node("pipeline", run_pipeline)
    graph.add_node("report", generate_report)
    graph.add_node("schema_info", show_schema)
    graph.add_node("help", show_help)
    graph.add_node("error", handle_error)

    graph.add_edge(START, "classify_intent")
    graph.add_edge("classify_intent", "load_schema")

    graph.add_conditional_edges(
        "load_schema",
        route_by_intent,
        {
            "ingest": "ingest",
            "transform": "transform",
            "analyze": "analyze",
            "query": "query",
            "generate_dbt": "generate_dbt",
            "pipeline": "pipeline",
            "report": "report",
            "schema_info": "schema_info",
            "help": "help",
            "error": "error",
        },
    )

    for node in ["ingest", "transform", "analyze", "query", "generate_dbt",
                 "pipeline", "report", "schema_info", "help", "error"]:
        graph.add_edge(node, END)

    return graph.compile()


# Module-level compiled graph singleton
graph = build_graph()
