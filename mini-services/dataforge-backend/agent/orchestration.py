"""
DataForge AI - LangGraph Orchestration
Multi-step agent workflow with conditional routing, error recovery, and state management.

Graph topology:
    START → classify_intent → load_schema → [conditional routing] → action_node → END

Supported intents: ingest, analyze, query, generate_dbt, pipeline, report, schema, help
"""

import os
import glob
from typing import Dict, Any, List, Optional, Literal
from typing_extensions import TypedDict
from datetime import datetime

from langgraph.graph import StateGraph, END

try:
    from langgraph.graph import START
except ImportError:
    # Fallback for older langgraph versions
    START = "__start__"

from agent.utils import (
    load_schema,
    save_schema,
    BASE_PATH,
    RAW_DATA_DIR,
    PIPELINES_DIR,
    REPORTS_DIR,
    format_result,
    validate_identifier,
    validate_sql,
)


# === State Definition ===

class AgentState(TypedDict):
    """Shared state across all graph nodes.

    Every node receives the full state and returns a partial dict that
    is merged into the state (overwriting keys that are returned).
    """

    # -- Input --
    command: str
    session_id: str

    # -- Intent classification --
    intent: str  # ingest | analyze | query | generate_dbt | pipeline | report | schema | help
    parsed_params: Dict[str, Any]

    # -- Schema / Data --
    schema: Dict[str, Any]
    dataset_type: str
    current_file: str
    current_table: str

    # -- LLM Results --
    analysis_result: Dict[str, Any]
    sql_result: Dict[str, Any]
    dbt_result: Dict[str, Any]

    # -- Pipeline --
    pipeline_steps: List[Dict[str, Any]]
    pipeline_logs: List[str]
    files_generated: List[str]

    # -- Execution --
    status: str  # pending | running | success | error
    error_message: Optional[str]
    duration: float
    timestamp: str

    # -- Messages (reserved for future conversation support) --
    messages: list


# === Keyword → Intent map (heuristic classifier) ===

_INTENT_KEYWORDS: Dict[str, List[str]] = {
    "ingest": ["upload", "ingest", "load", "import"],
    "analyze": ["analyze", "analysis", "inspect", "examine", "what is"],
    "query": [
        "query", "show", "select", "find", "top", "average", "sum",
        "count", "how many", "trend", "total", "best", "worst",
        "compare", "by region", "by product", "monthly", "yearly",
    ],
    "generate_dbt": ["dbt", "generate dbt", "create models", "transform models"],
    "pipeline": ["pipeline", "etl", "process", "run pipeline", "build pipeline"],
    "report": ["report", "summary", "export", "download"],
    "schema": ["schema", "columns", "structure", "describe"],
    "help": ["help", "what can you do", "commands", "list"],
}


# === Graph Nodes ===


async def classify_intent(state: AgentState) -> dict:
    """Classify the user command into an intent using keyword matching.

    In production this could be upgraded to an LLM-based classifier.
    """
    command = state.get("command", "").lower()

    for intent, keywords in _INTENT_KEYWORDS.items():
        if any(kw in command for kw in keywords):
            return {
                "intent": intent,
                "parsed_params": {"raw_command": state.get("command", "")},
            }

    # Default fallback: treat as pipeline
    return {
        "intent": "pipeline",
        "parsed_params": {"raw_command": state.get("command", "")},
    }


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
    """Ingest the most recent uploaded file into the DuckDB warehouse.

    Finds the newest CSV/JSON file in ``RAW_DATA_DIR``, detects its schema,
    and loads it into the warehouse via DuckDBTool.
    """
    from agent.tools.duckdb_tool import DuckDBTool

    try:
        # Find the most recently modified file in data/raw/
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

        # ingest_file handles schema detection + saving internally
        result = await duckdb.ingest_file(file_path)

        if "error" in result:
            return {
                "status": "error",
                "error_message": result["error"],
            }

        # Refresh schema after ingestion
        schema = load_schema()

        # Run transformation
        transform_result = await duckdb.transform()

        logs = [
            f"Ingested {filename}",
            f"Dataset type: {schema.get('dataset_type', 'unknown')}",
        ]
        if "message" in transform_result:
            logs.append(transform_result["message"])

        files = ["warehouse/warehouse.duckdb"]
        if "output_file" in transform_result:
            files.append(transform_result["output_file"])

        return {
            "schema": schema,
            "dataset_type": schema.get("dataset_type", "unknown"),
            "current_file": filename,
            "current_table": schema.get("table_name", "unknown"),
            "status": "success",
            "error_message": None,
            "files_generated": files,
            "pipeline_logs": logs,
        }

    except Exception as exc:
        return {
            "status": "error",
            "error_message": f"Ingestion failed: {exc}",
            "pipeline_logs": [f"ERROR during ingest: {exc}"],
        }


async def run_analysis(state: AgentState) -> dict:
    """Run LLM-powered analysis on the loaded dataset."""
    from agent.tools.llm_agent import LLMAgent

    try:
        llm = LLMAgent()
        schema = state.get("schema") or load_schema()
        session_id = state.get("session_id", "default")

        result = await llm.analyze_dataset(
            schema_info=schema,
            sample_data=[],
            session_id=session_id,
        )

        llm_used = result.get("llm_used", False)
        return {
            "analysis_result": result,
            "status": "success" if llm_used else "error",
            "error_message": None if llm_used else result.get("error", "LLM not available"),
        }

    except Exception as exc:
        return {
            "status": "error",
            "error_message": f"Analysis failed: {exc}",
            "pipeline_logs": state.get("pipeline_logs", []) + [f"ERROR during analysis: {exc}"],
        }


async def run_query(state: AgentState) -> dict:
    """Generate SQL from natural language and execute it."""
    from agent.tools.llm_agent import LLMAgent
    from agent.tools.duckdb_tool import DuckDBTool

    try:
        llm = LLMAgent()
        duckdb = DuckDBTool()
        schema = state.get("schema") or load_schema()
        command = state.get("command", "")
        session_id = state.get("session_id", "default")

        # Generate SQL
        sql_result = await llm.generate_sql(
            question=command,
            schema_info=schema,
            session_id=session_id,
        )

        # Execute SQL if generated
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

        return {
            "sql_result": sql_result,
            "status": "success",
        }

    except Exception as exc:
        return {
            "status": "error",
            "error_message": f"Query failed: {exc}",
            "pipeline_logs": state.get("pipeline_logs", []) + [f"ERROR during query: {exc}"],
        }


async def generate_dbt(state: AgentState) -> dict:
    """Generate dbt transformation models via LLM."""
    from agent.tools.llm_agent import LLMAgent

    try:
        llm = LLMAgent()
        schema = state.get("schema") or load_schema()
        session_id = state.get("session_id", "default")

        result = await llm.generate_dbt_models(
            schema_info=schema,
            session_id=session_id,
        )

        files = [m.get("path", "") for m in result.get("models", []) if m.get("path")]

        return {
            "dbt_result": result,
            "status": "success",
            "files_generated": files,
        }

    except Exception as exc:
        return {
            "status": "error",
            "error_message": f"dbt generation failed: {exc}",
            "pipeline_logs": state.get("pipeline_logs", []) + [f"ERROR during dbt generation: {exc}"],
        }


async def run_pipeline(state: AgentState) -> dict:
    """Execute the full data pipeline: ingest → transform → report."""
    from agent.pipeline_generator import PipelineGenerator
    from agent.tools.duckdb_tool import DuckDBTool
    from agent.tools.report_tool import ReportTool

    schema = state.get("schema") or load_schema()

    # Check for data first
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
                logs.append(f"[Ingest] Error: {ingest_result['error']}")
            else:
                logs.append(f"[Ingest] {ingest_result.get('message', f'Loaded {filename}')}")
                files.append("warehouse/warehouse.duckdb")
        else:
            logs.append("[Ingest] No files found in data/raw/")

        # Refresh schema after ingestion
        schema = load_schema()

        # --- Step 2: Transform ---
        transform_result = await duckdb.transform()
        if "error" in transform_result:
            logs.append(f"[Transform] Error: {transform_result['error']}")
        else:
            logs.append(f"[Transform] {transform_result.get('message', 'Completed')}")
            if "output_file" in transform_result:
                files.append(transform_result["output_file"])

        # --- Step 3: Generate pipeline code ---
        operations = ["ingest", "transform", "report"]
        pipeline_code = generator.generate(operations, schema)
        logs.append("[Pipeline] Pipeline code generated")
        files.append("pipelines/pipeline.py")

        # --- Step 4: Report ---
        report_msg = await reporter.generate()
        logs.append(f"[Report] {report_msg}")
        files.append("reports/report.csv")

        # Build simple step list
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
            "pipeline_logs": (state.get("pipeline_logs") or []) + [f"ERROR during pipeline: {exc}"],
        }


async def generate_report(state: AgentState) -> dict:
    """Generate a data report from the current warehouse."""
    from agent.tools.report_tool import ReportTool

    try:
        schema = state.get("schema") or load_schema()
        reporter = ReportTool()

        report_msg = await reporter.generate()

        # ReportTool.generate() returns a status string
        return {
            "status": "success",
            "files_generated": ["reports/report.csv"],
            "pipeline_logs": state.get("pipeline_logs", []) + [f"[Report] {report_msg}"],
        }

    except Exception as exc:
        return {
            "status": "error",
            "error_message": f"Report generation failed: {exc}",
            "pipeline_logs": state.get("pipeline_logs", []) + [f"ERROR during report: {exc}"],
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
        })

    return {
        "status": "success",
        "analysis_result": {
            "schema_info": {
                "dataset_type": schema.get("dataset_type", "unknown"),
                "table_name": schema.get("table_name", "unknown"),
                "row_count": schema.get("row_count", 0),
                "columns": columns_info,
            }
        },
    }


async def handle_error(state: AgentState) -> dict:
    """Error handler node — provides a friendly error message."""
    error = state.get("error_message", "Unknown error")
    return {
        "status": "error",
        "error_message": error,
        "pipeline_logs": state.get("pipeline_logs", []) + [f"ERROR: {error}"],
    }


async def show_help(state: AgentState) -> dict:
    """Return help information about available commands."""
    return {
        "status": "success",
        "analysis_result": {
            "help": {
                "available_intents": list(_INTENT_KEYWORDS.keys()),
                "examples": {
                    "ingest": "upload a CSV file",
                    "analyze": "analyze the dataset",
                    "query": "show top 10 by revenue",
                    "generate_dbt": "generate dbt models",
                    "pipeline": "run full data pipeline",
                    "report": "generate summary report",
                    "schema": "describe schema columns",
                    "help": "what can you do",
                },
            }
        },
    }


# === Conditional Router ===


def route_by_intent(state: AgentState) -> str:
    """Route to the appropriate action node based on classified intent.

    Checks for required preconditions (e.g., schema must be loaded for
    analyze/query/dbt/report intents).
    """
    intent = state.get("intent", "pipeline")
    schema = state.get("schema", {})
    has_schema = bool(schema and schema.get("columns"))

    if intent == "ingest":
        return "ingest"
    elif intent == "pipeline":
        return "pipeline"
    elif intent == "help":
        return "help"
    elif intent == "schema":
        return "schema_info"
    elif intent in ("analyze", "query", "generate_dbt", "report"):
        return intent if has_schema else "error"
    else:
        # Unknown intent → try pipeline
        return "pipeline"


# === Graph Builder ===


def build_graph() -> StateGraph:
    """Build and compile the DataForge agent workflow graph.

    Returns:
        Compiled LangGraph Runnable (invoke/ainvoke).
    """
    graph = StateGraph(AgentState)

    # --- Add nodes ---
    graph.add_node("classify_intent", classify_intent)
    graph.add_node("load_schema", load_current_schema)
    graph.add_node("ingest", ingest_data)
    graph.add_node("analyze", run_analysis)
    graph.add_node("query", run_query)
    graph.add_node("generate_dbt", generate_dbt)
    graph.add_node("pipeline", run_pipeline)
    graph.add_node("report", generate_report)
    graph.add_node("schema_info", show_schema)
    graph.add_node("help", show_help)
    graph.add_node("error", handle_error)

    # --- Entry edge ---
    graph.add_edge(START, "classify_intent")
    graph.add_edge("classify_intent", "load_schema")

    # --- Conditional routing after schema is loaded ---
    graph.add_conditional_edges(
        "load_schema",
        route_by_intent,
        {
            "ingest": "ingest",
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

    # --- Terminal edges (all action nodes → END) ---
    graph.add_edge("ingest", END)
    graph.add_edge("analyze", END)
    graph.add_edge("query", END)
    graph.add_edge("generate_dbt", END)
    graph.add_edge("pipeline", END)
    graph.add_edge("report", END)
    graph.add_edge("schema_info", END)
    graph.add_edge("help", END)
    graph.add_edge("error", END)

    return graph.compile()


# === Module-level compiled graph singleton ===
graph = build_graph()
