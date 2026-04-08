"""
Omnix - FastAPI Backend (Full LLM + XLSX + Airbyte)
Main application entry point - Works with ANY dataset
"""
from fastapi.responses import FileResponse   # ADD THIS LINE
from fastapi import FastAPI, HTTPException, UploadFile, File, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import json
import os
import sys
import shutil
import re
from datetime import datetime
import asyncio
import duckdb
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()

from agent.master_agent import MasterAgent
from agent.query_agent import QueryAgent
from agent.tools.duckdb_tool import DuckDBTool
from agent.tools.schema_detector import SchemaDetector
from agent.tools.llm_agent import LLMAgent
from agent.tools.xlsx_processor import XLSXProcessor
from agent.tools.airbyte_connector import AirbyteConnector
from agent.tools.report_tool import ReportTool
from agent.utils import (
    BASE_PATH,
    WAREHOUSE_DB_PATH,
    REPORTS_DIR,
    DBT_DIR,
    PIPELINES_DIR,
    CLEAN_DATA_DIR,
    load_schema,
    quote_identifier,
    validate_identifier,
)
from user_workspace import (
    get_user_workspace,
    load_user_schema,
    save_user_schema,
    resolve_user_relative_path,
    relative_to_user_root,
)
# ============ LLM Configuration ============
# Set your OpenAI API key here OR set OPENAI_API_KEY env variable
import os
if not os.getenv("OPENAI_API_KEY"):
    os.environ["OPENAI_API_KEY"] = ""  # Replace with your real key
app = FastAPI(
    title="Omnix Backend",
    description="AI-powered Data Engineering Platform - Works with ANY dataset",
    version="3.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize agents and tools
master_agent = MasterAgent()
query_agent = QueryAgent()
duckdb_tool = DuckDBTool()
schema_detector = SchemaDetector()
llm_agent = LLMAgent()
xlsx_processor = XLSXProcessor()
airbyte_connector = AirbyteConnector()
report_tool = ReportTool()

# ============ Request/Response Models ============

class CommandRequest(BaseModel):
    command: str
    user_id: Optional[str] = None
    active_file: Optional[str] = None


class QueryRequest(BaseModel):
    question: str
    user_id: Optional[str] = None
    active_file: Optional[str] = None


class LLMAnalysisRequest(BaseModel):
    use_llm: bool = True
    active_file: Optional[str] = None
    user_id: Optional[str] = None


class ActiveFileRequest(BaseModel):
    file_path: str
    user_id: Optional[str] = None


class AirbyteSourceRequest(BaseModel):
    name: str
    source_type: str
    connection_config: Dict[str, Any]


class AirbyteConnectionRequest(BaseModel):
    name: str
    source_id: str
    destination_id: str
    streams: Optional[List[Dict[str, Any]]] = None
    schedule: Optional[Dict[str, Any]] = None


class StatusResponse(BaseModel):
    status: str
    message: str
    data: Optional[Dict[str, Any]] = None


class FileInfo(BaseModel):
    name: str
    path: str
    type: str
    size: int
    content: Optional[str] = None


# ============ Health Check ============

@app.get("/")
async def root():
    return {"message": "Omnix Backend is running", "version": "3.0.0"}


@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


def _safe_relative(base: str, full_path: str) -> str:
    return os.path.relpath(full_path, base).replace("\\", "/")


def _resolve_workspace_file_path(user_id: Optional[str], file_path: str) -> str:
    """Resolve a file path with tenant-first and legacy fallback."""
    workspace = get_user_workspace(user_id)
    user_root = os.path.realpath(workspace["root"])
    base_root = os.path.realpath(BASE_PATH)

    candidates: List[str] = []
    if os.path.isabs(file_path):
        candidates.append(os.path.realpath(file_path))
    else:
        candidates.append(os.path.realpath(os.path.join(user_root, file_path)))
        candidates.append(os.path.realpath(os.path.join(base_root, file_path)))

    for candidate in candidates:
        if candidate.startswith(user_root + os.sep) and os.path.exists(candidate):
            return candidate
        if candidate.startswith(base_root + os.sep) and os.path.exists(candidate):
            return candidate
    raise FileNotFoundError(file_path)


async def _activate_file_context(file_path: str, user_id: Optional[str] = None) -> Dict[str, Any]:
    """Activate a specific file as the current working dataset."""
    workspace = get_user_workspace(user_id)
    user_root = os.path.realpath(workspace["root"])
    try:
        if os.path.isabs(file_path):
            full_path = os.path.realpath(file_path)
        else:
            full_path = resolve_user_relative_path(user_id, file_path)
    except ValueError:
        return {"error": "Access denied: path outside project directory"}
    if not full_path.startswith(user_root + os.sep):
        return {"error": "Access denied: path outside project directory"}
    if not os.path.exists(full_path):
        # Backward-compatible fallback for previously stored global paths like data/raw/*.csv
        legacy_candidate = os.path.realpath(os.path.join(BASE_PATH, file_path))
        if legacy_candidate.startswith(os.path.realpath(BASE_PATH) + os.sep) and os.path.exists(legacy_candidate):
            full_path = legacy_candidate
        else:
            return {"error": f"File not found: {file_path}"}

    ext = os.path.splitext(full_path)[1].lower()
    ingest_path = full_path

    # Convert Excel to CSV before ingestion.
    if ext in [".xlsx", ".xls", ".xlsm"]:
        convert_result = xlsx_processor.to_csv(full_path)
        output_files = convert_result.get("output_files", [])
        if not output_files:
            return {"error": "Failed to convert Excel file to CSV"}
        ingest_path = output_files[0]["path"]
    elif ext not in [".csv", ".json"]:
        return {"error": f"Unsupported active file type: {ext}"}

    schema = await schema_detector.detect_schema_from_file(ingest_path, use_llm=True)
    if "error" in schema:
        return {"error": schema["error"]}
    schema["source_file"] = relative_to_user_root(user_id, full_path)
    save_user_schema(user_id, schema)

    ingest_result = await duckdb_tool.ingest_file(ingest_path, user_id=user_id)
    if "error" in ingest_result:
        return {"error": ingest_result["error"]}

    return {
        "status": "success",
        "schema": load_user_schema(user_id),
        "ingested_path": ingest_path,
    }


# ============ SCHEMA DETECTION ROUTES ============

@app.get("/schema")
async def get_current_schema(user_id: Optional[str] = None):
    """Get the currently detected schema"""
    schema = await duckdb_tool.get_current_schema(user_id=user_id)
    return {
        "status": "success",
        "schema": schema
    }


@app.post("/schema/detect")
async def detect_schema(file_path: str, user_id: Optional[str] = None):
    """Detect schema from a specific file"""
    result = await schema_detector.detect_schema_from_file(file_path)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    if "error" not in result:
        save_user_schema(user_id, result)
    return {
        "status": "success",
        "schema": result
    }


@app.get("/schema/suggestions")
async def get_query_suggestions(user_id: Optional[str] = None):
    """Get suggested queries based on current schema"""
    suggestions = await query_agent.get_suggested_queries()
    schema = await duckdb_tool.get_current_schema(user_id=user_id)
    return {
        "status": "success",
        "suggestions": suggestions,
        "dataset_type": schema.get("dataset_type", "generic"),
        "table_name": schema.get("table_name", "data_clean")
    }


@app.post("/active-file")
async def set_active_file(request: ActiveFileRequest):
    """Set a selected file as the active dataset for all operations."""
    result = await _activate_file_context(request.file_path, user_id=request.user_id)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return {
        "status": "success",
        "message": f"Active file set to {request.file_path}",
        "schema": result.get("schema", {}),
    }


# ============ FILE UPLOAD ROUTES ============

@app.post("/upload")
async def upload_file(file: UploadFile = File(...), user_id: Optional[str] = None):
    """Upload a data file (CSV, JSON, or XLSX)"""
    # Validate file type
    allowed_extensions = ['.csv', '.json', '.xlsx', '.xls', '.xlsm']
    file_ext = os.path.splitext(file.filename)[1].lower()

    if file_ext not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type. Allowed: {', '.join(allowed_extensions)}"
        )

    # Save file using BASE_PATH from utils
    workspace = get_user_workspace(user_id)
    upload_dir = workspace["raw_dir"]
    os.makedirs(upload_dir, exist_ok=True)

    file_path = os.path.join(upload_dir, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Handle XLSX files differently
    if file_ext in ['.xlsx', '.xls', '.xlsm']:
        # Get sheet info
        sheet_names = xlsx_processor.get_sheet_names(file_path)
        preview = xlsx_processor.get_sheet_preview(file_path)

        # Convert to CSV for processing
        convert_result = xlsx_processor.to_csv(file_path)

        # Get schema from first sheet
        schema = xlsx_processor.get_schema_info(file_path)

        return {
            "status": "success",
            "message": f"Excel file uploaded: {file.filename}",
            "file_path": file_path,
            "sheets": sheet_names,
            "preview": preview,
            "schema": schema,
            "converted_files": convert_result.get("output_files", [])
        }

    # Detect schema for CSV/JSON
    schema = await schema_detector.detect_schema_from_file(file_path, use_llm=True)
    schema["source_file"] = relative_to_user_root(user_id, file_path)
    save_user_schema(user_id, schema)

    return {
        "status": "success",
        "message": f"File uploaded: {file.filename}",
        "file_path": file_path,
        "schema": schema
    }


@app.post("/upload-and-process")
async def upload_and_process(file: UploadFile = File(...), user_id: Optional[str] = None):
    """Upload a file — only saves, detects schema, and ingests raw data.
    Does NOT auto-transform, auto-generate pipeline, or auto-generate report.
    The user must issue commands through the agent to perform those actions.
    """
    try:
        # Validate file type
        allowed_extensions = ['.csv', '.json', '.xlsx', '.xls', '.xlsm']
        file_ext = os.path.splitext(file.filename)[1].lower()

        if file_ext not in allowed_extensions:
            raise HTTPException(status_code=400, detail="Unsupported file type")

        # Save file using BASE_PATH from utils
        workspace = get_user_workspace(user_id)
        upload_dir = workspace["raw_dir"]
        os.makedirs(upload_dir, exist_ok=True)

        file_path = os.path.join(upload_dir, file.filename)

        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Handle XLSX files
        csv_path = file_path
        if file_ext in ['.xlsx', '.xls', '.xlsm']:
            convert_result = xlsx_processor.to_csv(file_path)
            if convert_result.get("output_files"):
                csv_path = convert_result["output_files"][0]["path"]

        # Detect schema only
                # Detect schema (uses LLM if available, falls back to heuristic)
        schema = await schema_detector.detect_schema_from_file(csv_path, use_llm=True)
        schema["source_file"] = relative_to_user_root(user_id, file_path)
        save_user_schema(user_id, schema)

        if "error" in schema:
            return {"status": "error", "message": schema["error"]}

        # Ingest raw data into warehouse (creates {name}_raw table only)
        ingest_result = await duckdb_tool.ingest_file(csv_path, user_id=user_id)

        if "error" in ingest_result:
            return {"status": "error", "message": ingest_result["error"]}

        # STOP here — no auto-transform, no auto-pipeline, no auto-report
        # User must use the agent to give commands

        return {
            "status": "success",
            "message": f"File uploaded: {file.filename}. Use the agent to clean data, generate reports, or build pipelines.",
            "file_path": file_path,
            "schema": schema,
            "ingest": ingest_result,
            "agent_suggestions": [
                "Clean and transform the data",
                "Generate a summary report",
                "Create a full data pipeline",
                "Analyze the dataset",
                "Describe the schema",
            ]
        }
    except Exception as e:
        print(f"Upload error: {e}")
        return {"status": "error", "message": str(e)}


# ============ XLSX ROUTES ============

@app.post("/xlsx/upload")
async def upload_xlsx(file: UploadFile = File(...)):
    """Upload and process XLSX file with multi-sheet support"""
    file_ext = os.path.splitext(file.filename)[1].lower()

    if file_ext not in ['.xlsx', '.xls', '.xlsm']:
        raise HTTPException(status_code=400, detail="Please upload an Excel file (.xlsx, .xls)")

    # Read file content
    content = await file.read()

    # Process
    result = xlsx_processor.process_upload(content, file.filename, output_format="csv")

    return result


@app.get("/xlsx/sheets/{file_path:path}")
async def get_xlsx_sheets(file_path: str):
    """Get all sheets from an Excel file"""
    full_path = os.path.realpath(os.path.join(BASE_PATH, file_path))
    if not full_path.startswith(os.path.realpath(BASE_PATH) + os.sep):
        raise HTTPException(status_code=403, detail="Access denied: path outside project directory")
    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="File not found")

    sheets = xlsx_processor.get_sheet_names(full_path)
    return {"file_path": file_path, "sheets": sheets}


@app.get("/xlsx/preview/{file_path:path}")
async def preview_xlsx_sheet(file_path: str, sheet_name: str = None, rows: int = 10):
    """Preview data from an Excel sheet"""
    full_path = os.path.realpath(os.path.join(BASE_PATH, file_path))
    if not full_path.startswith(os.path.realpath(BASE_PATH) + os.sep):
        raise HTTPException(status_code=403, detail="Access denied: path outside project directory")
    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="File not found")

    preview = xlsx_processor.get_sheet_preview(full_path, sheet_name, rows)
    return preview


# ============ LLM ROUTES ============

@app.post("/llm/analyze")
async def analyze_dataset(request: Optional[LLMAnalysisRequest] = None):
    """Use LLM to analyze current dataset"""
    request = request or LLMAnalysisRequest()
    user_id = getattr(request, "user_id", None)
    if request.active_file:
        active_result = await _activate_file_context(request.active_file, user_id=user_id)
        if "error" in active_result:
            return {"status": "error", "message": active_result["error"]}

    schema = await duckdb_tool.get_current_schema(user_id=user_id)

    if not schema:
        return {"status": "error", "message": "No dataset loaded. Upload a file first."}

    # Get sample data
    sample_data = await duckdb_tool.get_sample_data(limit=100, user_id=user_id)

    # Analyze with LLM
    analysis = await llm_agent.analyze_dataset(
        schema,
        sample_data.get("data", [])
    )

    return {
        "status": "success",
        "schema": schema,
        "analysis": analysis
    }


@app.post("/llm/generate-dbt")
async def generate_dbt_models(user_id: Optional[str] = None):
    """Generate dbt models using LLM"""
    schema = load_user_schema(user_id)

    if not schema:
        return {"status": "error", "message": "No dataset loaded"}

    # Get analysis first
    sample_data = await duckdb_tool.get_sample_data(limit=100, user_id=user_id)
    analysis = await llm_agent.analyze_dataset(schema, sample_data.get("data", []))

    # Generate dbt models
    dbt_result = await llm_agent.generate_dbt_models(schema, analysis)

    # Save models to disk using DBT_DIR from utils
    saved_files = []
    for model in dbt_result.get("models", []):
        model_path = os.path.join(DBT_DIR, model["path"])
        os.makedirs(os.path.dirname(model_path), exist_ok=True)
        with open(model_path, 'w') as f:
            f.write(model["content"])
        saved_files.append(model["path"])

    # Save schema.yml
    if dbt_result.get("schema_yml"):
        schema_path = os.path.join(DBT_DIR, "models", "schema.yml")
        os.makedirs(os.path.dirname(schema_path), exist_ok=True)
        with open(schema_path, 'w') as f:
            f.write(dbt_result["schema_yml"])
        saved_files.append("models/schema.yml")

    return {
        "status": "success",
        "models": dbt_result.get("models", []),
        "schema_yml": dbt_result.get("schema_yml", ""),
        "saved_files": saved_files,
        "tests": dbt_result.get("tests", []),
        "documentation": dbt_result.get("documentation", "")
    }


@app.post("/llm/generate-pipeline")
async def generate_pipeline(operations: List[str] = None, user_id: Optional[str] = None):
    """Generate Prefect pipeline using LLM"""
    schema = load_user_schema(user_id)

    if not schema:
        return {"status": "error", "message": "No dataset loaded"}

    # Get analysis
    sample_data = await duckdb_tool.get_sample_data(limit=100, user_id=user_id)
    analysis = await llm_agent.analyze_dataset(schema, sample_data.get("data", []))

    # Generate pipeline
    pipeline_result = await llm_agent.generate_pipeline_code(schema, analysis, operations)

    # Save pipeline using PIPELINES_DIR from utils
    pipeline_path = os.path.join(PIPELINES_DIR, "pipeline_generated.py")
    os.makedirs(os.path.dirname(pipeline_path), exist_ok=True)

    with open(pipeline_path, 'w') as f:
        f.write(pipeline_result["pipeline_code"])

    return {
        "status": "success",
        "pipeline_path": "pipelines/pipeline_generated.py",
        "config": pipeline_result.get("config", {}),
        "description": pipeline_result.get("description", ""),
        "tasks": pipeline_result.get("tasks", [])
    }


# ============ AIRBYTE ROUTES ============

@app.get("/airbyte/sources")
async def list_airbyte_sources():
    """List all configured Airbyte sources"""
    await airbyte_connector.initialize()
    result = await airbyte_connector.list_sources()
    return result


@app.get("/airbyte/source-definitions")
async def list_source_definitions():
    """List available source connector types"""
    await airbyte_connector.initialize()
    result = await airbyte_connector.list_source_definitions()
    return result


@app.post("/airbyte/sources")
async def create_airbyte_source(request: AirbyteSourceRequest):
    """Create a new Airbyte source"""
    await airbyte_connector.initialize()

    result = await airbyte_connector.create_source(
        name=request.name,
        source_definition_id=request.source_type,
        connection_config=request.connection_config
    )

    return result


@app.post("/airbyte/sources/{source_id}/test")
async def test_source_connection(source_id: str):
    """Test connection to a source"""
    result = await airbyte_connector.test_source_connection(source_id)
    return result


@app.post("/airbyte/sources/{source_id}/discover")
async def discover_source_schema(source_id: str):
    """Discover available streams/tables from a source"""
    result = await airbyte_connector.discover_source_schema(source_id)
    return result


@app.get("/airbyte/destinations")
async def list_destinations():
    """List all destinations"""
    await airbyte_connector.initialize()
    result = await airbyte_connector.list_destinations()
    return result


@app.post("/airbyte/connections")
async def create_airbyte_connection(request: AirbyteConnectionRequest):
    """Create a connection between source and destination"""
    await airbyte_connector.initialize()

    result = await airbyte_connector.create_connection(
        name=request.name,
        source_id=request.source_id,
        destination_id=request.destination_id,
        streams=request.streams,
        schedule=request.schedule
    )

    return result


@app.get("/airbyte/connections")
async def list_airbyte_connections():
    """List all connections"""
    await airbyte_connector.initialize()
    result = await airbyte_connector.list_connections()
    return result


@app.post("/airbyte/connections/{connection_id}/sync")
async def sync_connection(connection_id: str, background_tasks: BackgroundTasks):
    """Trigger a sync for a connection"""
    result = await airbyte_connector.sync_connection(connection_id)
    return result


@app.get("/airbyte/connections/{connection_id}/status")
async def get_connection_status(connection_id: str):
    """Get status of a connection and its last sync"""
    result = await airbyte_connector.get_connection_info(connection_id)
    return result


@app.get("/airbyte/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Get status of a sync job"""
    result = await airbyte_connector.get_job_status(job_id)
    return result


@app.get("/airbyte/templates/{source_type}")
async def get_connection_template(source_type: str):
    """Get connection configuration template for a source type"""
    template = airbyte_connector.get_connection_template(source_type)
    return {
        "source_type": source_type,
        "template": template
    }


# ============ AGENT ROUTES ============

@app.post("/run-agent")
async def run_agent(request: CommandRequest):
    """
    Execute a natural language command through the master agent.
    The agent will parse the command and execute the appropriate pipeline.
    """
    import traceback
    try:
        if request.active_file:
            active_result = await _activate_file_context(request.active_file, user_id=request.user_id)
            if "error" in active_result:
                return {"status": "error", "message": active_result["error"]}

        cmd = (request.command or "").strip().lower()
        if any(token in cmd for token in ["clean", "transform", "preprocess"]):
            transform_result = await duckdb_tool.transform(user_id=request.user_id)
            if "error" in transform_result:
                return {"status": "error", "message": transform_result["error"], "data": transform_result}
            output_file = transform_result.get("output_file")
            files = []
            if output_file and os.path.exists(output_file):
                files.append(_safe_relative(get_user_workspace(request.user_id)["root"], output_file))
            step_logs = []
            for step in transform_result.get("cleaning_operations", []):
                col = step.get("column", step.get("operation", "step"))
                strategy = step.get("strategy", step.get("reason", "done"))
                before = step.get("missing_before")
                after = step.get("missing_after")
                if before is not None and after is not None:
                    step_logs.append(f"[Clean] {col}: {strategy} ({before} -> {after})")
                else:
                    step_logs.append(f"[Clean] {col}: {strategy}")
            return {
                "status": "success",
                "message": transform_result.get("message", "Data transformed successfully"),
                "data": {
                    **transform_result,
                    "files": files,
                    "files_generated": files,
                    "logs": step_logs,
                }
            }
        if any(token in cmd for token in ["report", "dashboard", "summary"]):
            report_message = await report_tool.generate(schema=load_user_schema(request.user_id), user_id=request.user_id)
            if "Table" in str(report_message) and "not found" in str(report_message):
                transform_result = await duckdb_tool.transform(user_id=request.user_id)
                if "error" not in transform_result:
                    report_message = await report_tool.generate(schema=load_user_schema(request.user_id), user_id=request.user_id)
            if str(report_message).startswith("Error"):
                return {"status": "error", "message": report_message}
            schema = load_user_schema(request.user_id)
            files = []
            report_csv = schema.get("report_file")
            report_html = schema.get("report_html_file")
            if report_csv and os.path.exists(report_csv):
                files.append(_safe_relative(get_user_workspace(request.user_id)["root"], report_csv))
            if report_html and os.path.exists(report_html):
                files.append(_safe_relative(get_user_workspace(request.user_id)["root"], report_html))
            return {
                "status": "success",
                "message": report_message,
                "data": {
                    "status": "success",
                    "message": report_message,
                    "files": files,
                    "files_generated": files,
                    "logs": [f"[Report] {report_message}"],
                },
            }
        result = await master_agent.execute(request.command, session_id=request.user_id or "default")
        return result
    except Exception as e:
        print(f"Error in run_agent: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status")
async def get_status(user_id: Optional[str] = None):
    """Get the last execution status"""
    schema = await duckdb_tool.get_current_schema(user_id=user_id)
    return {
        "status": "ready",
        "message": "System is operational",
        "last_execution": master_agent.last_execution,
        "pipelines_count": len(master_agent.pipelines),
        "current_dataset": schema.get("dataset_type", "none"),
        "current_table": schema.get("table_name", "none")
    }


# ============ QUERY ROUTES ============

@app.post("/query")
async def execute_query(request: QueryRequest):
    """
    Execute a natural language query on the data warehouse.
    Converts natural language to SQL and executes on DuckDB.
    Uses LLM for intelligent SQL generation.
    """
    try:
        if request.active_file:
            active_result = await _activate_file_context(request.active_file, user_id=request.user_id)
            if "error" in active_result:
                raise HTTPException(status_code=400, detail=active_result["error"])

        # Get current schema
        schema = await duckdb_tool.get_current_schema(user_id=request.user_id)

        if not schema:
            raise HTTPException(status_code=400, detail="No dataset loaded")

        # Use LLM for SQL generation
        llm_result = await llm_agent.generate_sql_from_question(
            request.question,
            schema
        )

        sql = llm_result.get("sql", "")

        if not sql:
            raise HTTPException(status_code=400, detail="Failed to generate SQL for the question")
        else:
            # Execute LLM-generated SQL
            query_result = await duckdb_tool.query(sql, user_id=request.user_id)

            if query_result.get("status") == "error":
                raise HTTPException(status_code=400, detail=query_result.get("message", "Query execution failed"))
            result = {
                "sql": sql,
                "columns": query_result.get("columns", []),
                "data": query_result.get("data", []),
                "execution_time": 0,
                "schema": schema,
                "explanation": llm_result.get("explanation", ""),
                "generated_by": "llm"
            }

        return {
            "status": "success",
            "sql": result["sql"],
            "columns": result["columns"],
            "data": result["data"],
            "execution_time": result.get("execution_time", 0),
            "schema_info": result.get("schema", {}),
            "explanation": result.get("explanation", ""),
            "generated_by": result.get("generated_by", "pattern")
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ FILE ROUTES ============

@app.get("/files")
async def list_files(user_id: Optional[str] = None):
    """List all generated files."""
    workspace = get_user_workspace(user_id)
    base_workspace = get_user_workspace(None)
    file_index: Dict[str, Dict[str, Any]] = {}

    def add_file(full_path: str, relative_path: str, category: str):
        if not os.path.exists(full_path):
            return
        ext = os.path.splitext(full_path)[1].lstrip(".").lower() or "unknown"
        modified_ts = os.path.getmtime(full_path)
        existing = file_index.get(relative_path)
        if existing and datetime.fromisoformat(existing["modified"]).timestamp() >= modified_ts:
            return
        file_index[relative_path] = {
            "name": os.path.basename(full_path),
            "path": relative_path.replace("\\", "/"),
            "type": ext,
            "category": category,
            "size": os.path.getsize(full_path),
            "modified": datetime.fromtimestamp(modified_ts).isoformat(),
        }

    def scan_dir(disk_dir: str, logical_prefix: str, category: str, allowed_ext: set[str]):
        if not os.path.exists(disk_dir):
            return
        for name in os.listdir(disk_dir):
            full_path = os.path.join(disk_dir, name)
            if not os.path.isfile(full_path):
                continue
            ext = os.path.splitext(name)[1].lower()
            if allowed_ext and ext not in allowed_ext:
                continue
            add_file(full_path, f"{logical_prefix}/{name}", category)

    scan_dir(workspace["raw_dir"], "data/raw", "raw_data", {".csv", ".json", ".xlsx", ".xls", ".xlsm"})
    scan_dir(workspace["clean_dir"], "data/clean", "clean_data", {".csv", ".parquet", ".json"})
    scan_dir(workspace["reports_dir"], "reports", "report", {".csv", ".html", ".json"})
    scan_dir(os.path.join(workspace["root"], "pipelines"), "pipelines", "pipeline", {".py", ".sql", ".yml", ".yaml", ".json"})

    if workspace["root"] != base_workspace["root"]:
        scan_dir(base_workspace["raw_dir"], "data/raw", "raw_data", {".csv", ".json", ".xlsx", ".xls", ".xlsm"})
        scan_dir(base_workspace["clean_dir"], "data/clean", "clean_data", {".csv", ".parquet", ".json"})
        scan_dir(base_workspace["reports_dir"], "reports", "report", {".csv", ".html", ".json"})
        scan_dir(os.path.join(base_workspace["root"], "pipelines"), "pipelines", "pipeline", {".py", ".sql", ".yml", ".yaml", ".json"})

    add_file(workspace["warehouse_db"], "warehouse/warehouse.duckdb", "warehouse")
    add_file(workspace["schema_cache"], "configs/schema_cache.json", "schema")
    add_file(os.path.join(workspace["configs_dir"], "transformation_log.json"), "configs/transformation_log.json", "schema")
    if workspace["root"] != base_workspace["root"]:
        add_file(base_workspace["warehouse_db"], "warehouse/warehouse.duckdb", "warehouse")
        add_file(base_workspace["schema_cache"], "configs/schema_cache.json", "schema")

    dbt_models_dir = os.path.join(DBT_DIR, "models")
    if os.path.exists(dbt_models_dir):
        for root, _, filenames in os.walk(dbt_models_dir):
            for name in filenames:
                if not name.endswith((".sql", ".yml", ".yaml")):
                    continue
                full_path = os.path.join(root, name)
                rel_path = _safe_relative(BASE_PATH, full_path)
                add_file(full_path, rel_path, "dbt_model")

    files = sorted(file_index.values(), key=lambda x: x["modified"], reverse=True)
    return {"files": files, "count": len(files)}
@app.get("/files/{file_path:path}")
async def get_file(file_path: str, user_id: Optional[str] = None):
    """Get a specific file by path (safe - prevents path traversal)"""
    try:
        full_path = _resolve_workspace_file_path(user_id, file_path)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")

    size = os.path.getsize(full_path)
    content = None

    try:
        with open(full_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except (UnicodeDecodeError, IOError, OSError):
        content = "[Binary file - cannot display]"

    return {
        "name": os.path.basename(file_path),
        "path": file_path,
        "type": file_path.split('.')[-1] if '.' in file_path else "unknown",
        "size": size,
        "content": content,
        "modified": datetime.fromtimestamp(os.path.getmtime(full_path)).isoformat()
    }


# ============ PIPELINE ROUTES ============

@app.get("/pipelines")
async def list_pipelines():
    """List all pipelines"""
    return {
        "pipelines": master_agent.pipelines,
        "count": len(master_agent.pipelines)
    }


@app.post("/pipelines/execute")
async def execute_pipeline(request: CommandRequest):
    """Execute a specific pipeline"""
    try:
        result = await master_agent.execute_pipeline(request.command)
        return {
            "status": "success",
            "message": "Pipeline executed successfully",
            "data": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pipelines/generate")
async def generate_pipeline_endpoint(file_path: str = None, user_id: Optional[str] = None):
    """Generate a pipeline for the current or specified data file"""
    from agent.pipeline_generator import PipelineGenerator

    generator = PipelineGenerator()

    if file_path:
        result = await generator.generate_from_file(file_path)
    else:
        schema = load_user_schema(user_id)
        if schema:
            result = {
                "status": "success",
                "schema": schema,
                "pipeline_code": generator.generate(["ingest", "transform", "report"], schema)
            }
        else:
            return {"status": "error", "message": "No schema found. Upload a file first."}

    return result


# ============ DASHBOARD ROUTES ============

@app.get("/dashboard/stats")
async def get_dashboard_stats(user_id: Optional[str] = None):
    """Get dashboard statistics"""
    schema = await duckdb_tool.get_current_schema(user_id=user_id)
    workspace = get_user_workspace(user_id)

    # LLM health check (sync method - no await)
    llm_health = llm_agent.check_health() if hasattr(llm_agent, 'check_health') else {"available": False}

    stats = {
        "total_pipelines": len(master_agent.pipelines),
        "total_executions": master_agent.execution_count,
        "success_rate": 100.0,
        "tables": 0,
        "reports": 0,
        "data_volume": 0,
        "dataset_type": schema.get("dataset_type", "none") if schema else "none",
        "current_table": schema.get("table_name", "none") if schema else "none",
        "features": {
            "llm_enabled": llm_health.get("available", False),
            "xlsx_support": True,
            "airbyte_connected": airbyte_connector._initialized
        }
    }

    # Calculate success_rate from actual pipeline history
    if master_agent.pipelines:
        success_count = sum(1 for p in master_agent.pipelines if p.get("status") == "success")
        stats["success_rate"] = round(success_count / len(master_agent.pipelines) * 100, 1)

    # Get warehouse stats using WAREHOUSE_DB_PATH from utils
    if os.path.exists(workspace["warehouse_db"]):
        try:
            con = duckdb.connect(workspace["warehouse_db"], read_only=True)
            tables = con.execute("SHOW TABLES").fetchall()
            stats["tables"] = len(tables)

            # Get row count from current table with safe identifier
            if schema:
                table_name = schema.get("table_name", "data_clean")
                if validate_identifier(table_name):
                    try:
                        quoted_table = quote_identifier(table_name)
                        row_count = con.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0]
                        stats["data_volume"] = row_count
                    except duckdb.Error:
                        pass
            con.close()
        except (duckdb.Error, OSError):
            pass

    # Count reports using REPORTS_DIR from utils
    if os.path.exists(workspace["reports_dir"]):
        stats["reports"] = len([f for f in os.listdir(workspace["reports_dir"]) if f.endswith('.csv')])

    return stats


@app.get("/dashboard/charts")
async def get_chart_data(user_id: Optional[str] = None):
    """Get data for dashboard charts - Dynamic based on schema"""
    schema = await duckdb_tool.get_current_schema(user_id=user_id)
    workspace = get_user_workspace(user_id)

    # Build pipeline_runs from actual pipeline history
    pipeline_runs = []
    if master_agent.pipelines:
        # Group pipelines by month for chart data
        monthly_data: Dict[str, Dict[str, int]] = {}
        for p in master_agent.pipelines:
            timestamp = p.get("timestamp", "")
            if timestamp:
                try:
                    month_key = timestamp[:7]  # YYYY-MM
                except (TypeError, IndexError):
                    month_key = "unknown"
            else:
                month_key = "unknown"

            if month_key not in monthly_data:
                monthly_data[month_key] = {"runs": 0, "success": 0}
            monthly_data[month_key]["runs"] += 1
            if p.get("status") == "success":
                monthly_data[month_key]["success"] += 1

        for month in sorted(monthly_data.keys()):
            d = monthly_data[month]
            pipeline_runs.append({
                "date": month,
                "runs": d["runs"],
                "success": d["success"],
            })

    charts = {
        "pipeline_runs": pipeline_runs,
        "primary_chart": [],
        "secondary_chart": [],
        "trend_chart": []
    }

    if not schema or not os.path.exists(workspace["warehouse_db"]):
        return charts

    table_name = schema.get("table_name", "data_clean")
    columns = schema.get("columns", {})
    dataset_type = schema.get("dataset_type", "generic")

    # Validate table name before use in SQL
    if not validate_identifier(table_name):
        return charts

    quoted_table = quote_identifier(table_name)

    # Categorize columns
    numeric_cols = []
    category_cols = []
    date_cols = []

    for col_name, col_info in columns.items():
        # Skip columns with unsafe identifiers
        if not validate_identifier(col_name):
            continue

        semantic = col_info.get("semantic", "generic")
        dtype = col_info.get("type", "").lower()

        if semantic in ["money", "count", "score", "percentage"] or \
           "int" in dtype or "float" in dtype or "double" in dtype:
            numeric_cols.append(col_name)
        elif semantic in ["category", "location", "name"]:
            category_cols.append(col_name)
        elif semantic == "datetime" or "date" in dtype:
            date_cols.append(col_name)

    try:
        con = duckdb.connect(workspace["warehouse_db"], read_only=True)

        # Primary chart: Group by category with aggregation (using quote_identifier)
        if category_cols and numeric_cols:
            try:
                quoted_cat = quote_identifier(category_cols[0])
                quoted_num = quote_identifier(numeric_cols[0])
                result = con.execute(f"""
                    SELECT {quoted_cat}, SUM({quoted_num}) as total
                    FROM {quoted_table}
                    GROUP BY {quoted_cat}
                    ORDER BY total DESC
                    LIMIT 10
                """).fetchall()
                charts["primary_chart"] = [
                    {"category": str(r[0]), "value": float(r[1])} for r in result
                ]
            except (duckdb.Error, ValueError, TypeError):
                pass

        # Secondary chart: Distribution by another category (using quote_identifier)
        if len(category_cols) > 1:
            try:
                quoted_cat2 = quote_identifier(category_cols[1])
                result = con.execute(f"""
                    SELECT {quoted_cat2}, COUNT(*) as count
                    FROM {quoted_table}
                    GROUP BY {quoted_cat2}
                    ORDER BY count DESC
                    LIMIT 10
                """).fetchall()
                charts["secondary_chart"] = [
                    {"category": str(r[0]), "value": int(r[1])} for r in result
                ]
            except (duckdb.Error, ValueError, TypeError):
                pass

        # Trend chart: Over time (using quote_identifier)
        if date_cols and numeric_cols:
            try:
                quoted_date = quote_identifier(date_cols[0])
                quoted_num = quote_identifier(numeric_cols[0])
                result = con.execute(f"""
                    SELECT strftime({quoted_date}, '%Y-%m') as period,
                           SUM({quoted_num}) as total
                    FROM {quoted_table}
                    GROUP BY period
                    ORDER BY period
                    LIMIT 12
                """).fetchall()
                charts["trend_chart"] = [
                    {"period": str(r[0]), "value": float(r[1])} for r in result
                ]
            except (duckdb.Error, ValueError, TypeError):
                pass

        con.close()
    except (duckdb.Error, OSError):
        pass

    return charts


# ============ WAREHOUSE ROUTES ============

@app.get("/warehouse/tables")
async def list_warehouse_tables(user_id: Optional[str] = None):
    """List all tables in the warehouse"""
    tables = []
    workspace = get_user_workspace(user_id)
    if os.path.exists(workspace["warehouse_db"]):
        try:
            con = duckdb.connect(workspace["warehouse_db"], read_only=True)
            table_list = con.execute("SHOW TABLES").fetchall()

            for table in table_list:
                table_name = table[0]
                if not validate_identifier(table_name):
                    continue
                try:
                    quoted_name = quote_identifier(table_name)
                    columns = con.execute(f"DESCRIBE {quoted_name}").fetchall()
                    row_count = con.execute(f"SELECT COUNT(*) FROM {quoted_name}").fetchone()[0]

                    tables.append({
                        "name": table_name,
                        "row_count": row_count,
                        "columns": [{"name": col[0], "type": col[1]} for col in columns]
                    })
                except duckdb.Error:
                    pass

            con.close()
        except (duckdb.Error, OSError):
            pass

    return {"tables": tables, "count": len(tables)}


@app.get("/warehouse/tables/{table_name}")
async def get_table_data(table_name: str, limit: int = 100, user_id: Optional[str] = None):
    """Get data from a specific table"""
    if not validate_identifier(table_name):
        raise HTTPException(status_code=400, detail=f"Invalid table name: {table_name!r}")

    workspace = get_user_workspace(user_id)
    if not os.path.exists(workspace["warehouse_db"]):
        raise HTTPException(status_code=404, detail="Warehouse not found")

    try:
        con = duckdb.connect(workspace["warehouse_db"], read_only=True)

        # Get columns using safe identifier
        quoted_table = quote_identifier(table_name)
        columns = [col[0] for col in con.execute(f"DESCRIBE {quoted_table}").fetchall()]

        # Validate limit
        try:
            limit = int(limit)
            if limit < 1 or limit > 10000:
                limit = 100
        except (TypeError, ValueError):
            limit = 100

        # Get data
        data = con.execute(f"SELECT * FROM {quoted_table} LIMIT {limit}").fetchall()

        con.close()

        return {
            "table": table_name,
            "columns": columns,
            "data": [dict(zip(columns, row)) for row in data],
            "row_count": len(data)
        }
    except duckdb.Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/warehouse/sample")
async def get_warehouse_sample(limit: int = 20, user_id: Optional[str] = None):
    """Get sample data from the current table"""
    result = await duckdb_tool.get_sample_data(limit=limit, user_id=user_id)
    return result


@app.get("/report/view")
async def view_report(
    user_id: Optional[str] = None,
    limit: int = 200,
    filter_column: Optional[str] = None,
    filter_value: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_dir: str = "asc",
    bar_category: Optional[str] = None,
    bar_metric: Optional[str] = None,
):
    schema = await duckdb_tool.get_current_schema(user_id=user_id)
    table_name = schema.get("table_name", "data_clean")
    if not validate_identifier(table_name):
        raise HTTPException(status_code=400, detail="Invalid table name")

    workspace = get_user_workspace(user_id)
    if not os.path.exists(workspace["warehouse_db"]):
        raise HTTPException(status_code=404, detail="Warehouse not found")

    con = duckdb.connect(workspace["warehouse_db"], read_only=True)
    try:
        tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
        selected_table = table_name
        if selected_table not in tables:
            raw_candidate = schema.get("raw_table", "")
            if raw_candidate in tables:
                selected_table = raw_candidate
            else:
                con.close()
                transform_result = await duckdb_tool.transform(user_id=user_id)
                if "error" in transform_result:
                    raise HTTPException(status_code=404, detail=f"No reportable table found: {table_name}")
                schema = await duckdb_tool.get_current_schema(user_id=user_id)
                selected_table = schema.get("table_name", table_name)
                con = duckdb.connect(workspace["warehouse_db"], read_only=True)
                tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
                if selected_table not in tables:
                    if raw_candidate in tables:
                        selected_table = raw_candidate
                    else:
                        raise HTTPException(status_code=404, detail=f"No reportable table found: {selected_table}")

        quoted_table = quote_identifier(selected_table)
        df = con.execute(f"SELECT * FROM {quoted_table}").fetchdf()
    finally:
        con.close()

    if filter_column and filter_value and filter_column in df.columns:
        series = df[filter_column]
        numeric_series = pd.to_numeric(series, errors="coerce")
        val = str(filter_value).strip()
        m = re.match(r"^\s*(>=|<=|>|<|=)?\s*(-?\d+(?:\.\d+)?)\s*$", val)
        if m and numeric_series.notna().mean() > 0.8:
            op = m.group(1) or "="
            num = float(m.group(2))
            if op == ">":
                df = df[numeric_series > num]
            elif op == "<":
                df = df[numeric_series < num]
            elif op == ">=":
                df = df[numeric_series >= num]
            elif op == "<=":
                df = df[numeric_series <= num]
            else:
                df = df[numeric_series == num]
        else:
            df = df[series.astype(str).str.contains(val, case=False, na=False)]

    if sort_by and sort_by in df.columns:
        df = df.sort_values(by=sort_by, ascending=sort_dir.lower() != "desc")

    numeric_cols = list(df.select_dtypes(include=["number"]).columns)
    non_numeric_cols = [c for c in df.columns if c not in numeric_cols]

    bar = []
    pie = []
    line = []
    histogram = []
    heatmap = []

    selected_cat = bar_category if bar_category in df.columns else (non_numeric_cols[0] if non_numeric_cols else (df.columns[0] if len(df.columns) else None))
    selected_metric = bar_metric if bar_metric in numeric_cols or bar_metric == "__count__" else (numeric_cols[0] if numeric_cols else "__count__")

    if selected_cat:
        if selected_metric == "__count__":
            grouped = df.groupby(selected_cat, dropna=False).size().reset_index(name="__value__").sort_values("__value__", ascending=False).head(12)
            bar = [{"category": str(r[selected_cat]), "value": float(r["__value__"])} for _, r in grouped.iterrows()]
            pie = [{"name": str(r[selected_cat]), "value": float(r["__value__"])} for _, r in grouped.head(6).iterrows()]
        elif selected_metric in numeric_cols:
            grouped = df.groupby(selected_cat, dropna=False)[selected_metric].sum().reset_index().sort_values(selected_metric, ascending=False).head(12)
            bar = [{"category": str(r[selected_cat]), "value": float(r[selected_metric])} for _, r in grouped.iterrows()]
            pie = [{"name": str(r[selected_cat]), "value": float(r[selected_metric])} for _, r in grouped.head(6).iterrows()]

    if numeric_cols:
        num = numeric_cols[0]
        hist = pd.cut(df[num], bins=12).value_counts(sort=False)
        histogram = [{"bucket": str(k), "count": int(v)} for k, v in hist.items()]
        corr = df[numeric_cols].corr().fillna(0)
        for row in corr.index:
            for col in corr.columns:
                heatmap.append({"x": str(col), "y": str(row), "value": float(round(corr.loc[row, col], 4))})

    datetime_candidates = []
    for col in df.columns:
        non_null = df[col].dropna()
        if non_null.empty:
            continue
        as_str = non_null.astype(str).str.strip().head(min(200, len(non_null)))
        is_date_like = as_str.str.contains(
            r"(?:\d{1,4}[-/]\d{1,2}[-/]\d{1,4}|\d{1,2}:\d{2}|[A-Za-z]{3,9}\s+\d{1,2})",
            regex=True,
        ).mean() >= 0.6
        if not is_date_like:
            continue
        try:
            parsed = pd.to_datetime(df[col], errors="coerce", format="mixed")
        except TypeError:
            parsed = pd.to_datetime(df[col], errors="coerce")
        if parsed.notna().mean() > 0.7:
            datetime_candidates.append(col)
            df[col] = parsed
    if datetime_candidates and numeric_cols:
        dt_col = datetime_candidates[0]
        num = numeric_cols[0]
        trend_df = df.dropna(subset=[dt_col]).copy()
        if not trend_df.empty:
            trend_df["_period"] = trend_df[dt_col].dt.to_period("M").astype(str)
            grouped = trend_df.groupby("_period")[num].sum().reset_index().sort_values("_period").tail(12)
            line = [{"period": str(r["_period"]), "value": float(r[num])} for _, r in grouped.iterrows()]

    return {
        "status": "success",
        "table": selected_table,
        "columns": [{"name": c, "type": str(df[c].dtype)} for c in df.columns],
        "rows": df.head(limit).fillna("").to_dict("records"),
        "total_rows": int(len(df)),
        "report_file": schema.get("report_file"),
        "report_html_file": schema.get("report_html_file"),
        "charts": {
            "bar": bar,
            "line": line,
            "pie": pie,
            "histogram": histogram,
            "heatmap": heatmap,
        },
        "bar_options": {
            "selected_category": selected_cat,
            "selected_metric": selected_metric,
            "category_candidates": [str(c) for c in df.columns],
            "metric_candidates": ["__count__"] + [str(c) for c in numeric_cols],
        },
    }


@app.get("/report/drilldown")
async def report_drilldown(
    user_id: Optional[str] = None,
    group_by: str = "",
    group_value: str = "",
    limit: int = 100,
):
    if not group_by:
        raise HTTPException(status_code=400, detail="group_by is required")
    schema = await duckdb_tool.get_current_schema(user_id=user_id)
    table_name = schema.get("table_name", "data_clean")
    if not validate_identifier(table_name):
        raise HTTPException(status_code=400, detail="Invalid table name")

    workspace = get_user_workspace(user_id)
    if not os.path.exists(workspace["warehouse_db"]):
        raise HTTPException(status_code=404, detail="Warehouse not found")

    con = duckdb.connect(workspace["warehouse_db"], read_only=True)
    quoted_table = quote_identifier(table_name)
    try:
        df = con.execute(f"SELECT * FROM {quoted_table}").fetchdf()
    finally:
        con.close()

    if group_by not in df.columns:
        raise HTTPException(status_code=400, detail="Invalid group_by column")

    out = df[df[group_by].astype(str) == str(group_value)].head(limit)
    return {"status": "success", "rows": out.fillna("").to_dict("records"), "count": int(len(out))}

@app.get("/download/{file_path:path}")
async def download_file(file_path: str, user_id: Optional[str] = None):
    """Download a file with proper Content-Disposition header"""

    try:
        full_path = _resolve_workspace_file_path(user_id, file_path)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")

    filename = os.path.basename(full_path)
    ext = os.path.splitext(filename)[1].lower()
    media_type = "application/octet-stream"
    if ext == ".html":
        media_type = "text/html; charset=utf-8"
    elif ext == ".json":
        media_type = "application/json"
    elif ext == ".csv":
        media_type = "text/csv; charset=utf-8"
    return FileResponse(
        path=full_path,
        filename=filename,
        media_type=media_type
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3001)

