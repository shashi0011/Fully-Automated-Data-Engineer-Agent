"""
DataForge AI - FastAPI Backend (Full LLM + XLSX + Airbyte)
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
from datetime import datetime
import asyncio
import duckdb

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
# ============ LLM Configuration ============
# Set your OpenAI API key here OR set OPENAI_API_KEY env variable
import os
if not os.getenv("OPENAI_API_KEY"):
    os.environ["OPENAI_API_KEY"] = ""  # Replace with your real key
app = FastAPI(
    title="DataForge AI Backend",
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


class ActiveFileRequest(BaseModel):
    file_path: str


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
    return {"message": "DataForge AI Backend is running", "version": "3.0.0"}


@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


async def _activate_file_context(file_path: str) -> Dict[str, Any]:
    """Activate a specific file as the current working dataset."""
    if os.path.isabs(file_path):
        full_path = os.path.realpath(file_path)
    else:
        full_path = os.path.realpath(os.path.join(BASE_PATH, file_path))
    if not full_path.startswith(os.path.realpath(BASE_PATH) + os.sep):
        return {"error": "Access denied: path outside project directory"}
    if not os.path.exists(full_path):
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

    ingest_result = await duckdb_tool.ingest_file(ingest_path)
    if "error" in ingest_result:
        return {"error": ingest_result["error"]}

    return {
        "status": "success",
        "schema": load_schema(),
        "ingested_path": ingest_path,
    }


# ============ SCHEMA DETECTION ROUTES ============

@app.get("/schema")
async def get_current_schema():
    """Get the currently detected schema"""
    schema = await duckdb_tool.get_current_schema()
    return {
        "status": "success",
        "schema": schema
    }


@app.post("/schema/detect")
async def detect_schema(file_path: str):
    """Detect schema from a specific file"""
    result = await schema_detector.detect_schema_from_file(file_path)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return {
        "status": "success",
        "schema": result
    }


@app.get("/schema/suggestions")
async def get_query_suggestions():
    """Get suggested queries based on current schema"""
    suggestions = await query_agent.get_suggested_queries()
    schema = await query_agent.get_current_schema()
    return {
        "status": "success",
        "suggestions": suggestions,
        "dataset_type": schema.get("dataset_type", "generic"),
        "table_name": schema.get("table_name", "data_clean")
    }


@app.post("/active-file")
async def set_active_file(request: ActiveFileRequest):
    """Set a selected file as the active dataset for all operations."""
    result = await _activate_file_context(request.file_path)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return {
        "status": "success",
        "message": f"Active file set to {request.file_path}",
        "schema": result.get("schema", {}),
    }


# ============ FILE UPLOAD ROUTES ============

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
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
    upload_dir = os.path.join(BASE_PATH, "data/raw")
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

    return {
        "status": "success",
        "message": f"File uploaded: {file.filename}",
        "file_path": file_path,
        "schema": schema
    }


@app.post("/upload-and-process")
async def upload_and_process(file: UploadFile = File(...)):
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
        upload_dir = os.path.join(BASE_PATH, "data/raw")
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

        if "error" in schema:
            return {"status": "error", "message": schema["error"]}

        # Ingest raw data into warehouse (creates {name}_raw table only)
        ingest_result = await duckdb_tool.ingest_file(csv_path)

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
    if request.active_file:
        active_result = await _activate_file_context(request.active_file)
        if "error" in active_result:
            return {"status": "error", "message": active_result["error"]}

    schema = schema_detector.load_schema_cache()

    if not schema:
        return {"status": "error", "message": "No dataset loaded. Upload a file first."}

    # Get sample data
    sample_data = await duckdb_tool.get_sample_data(limit=100)

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
async def generate_dbt_models():
    """Generate dbt models using LLM"""
    schema = schema_detector.load_schema_cache()

    if not schema:
        return {"status": "error", "message": "No dataset loaded"}

    # Get analysis first
    sample_data = await duckdb_tool.get_sample_data(limit=100)
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
async def generate_pipeline(operations: List[str] = None):
    """Generate Prefect pipeline using LLM"""
    schema = schema_detector.load_schema_cache()

    if not schema:
        return {"status": "error", "message": "No dataset loaded"}

    # Get analysis
    sample_data = await duckdb_tool.get_sample_data(limit=100)
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
            active_result = await _activate_file_context(request.active_file)
            if "error" in active_result:
                return {"status": "error", "message": active_result["error"]}
        result = await master_agent.execute(request.command)
        return {
            "status": "success",
            "message": f"Command executed: {request.command}",
            "data": result
        }
    except Exception as e:
        print(f"Error in run_agent: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status")
async def get_status():
    """Get the last execution status"""
    schema = await duckdb_tool.get_current_schema()
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
            active_result = await _activate_file_context(request.active_file)
            if "error" in active_result:
                raise HTTPException(status_code=400, detail=active_result["error"])

        # Get current schema
        schema = schema_detector.load_schema_cache()

        if not schema:
            raise HTTPException(status_code=400, detail="No dataset loaded")

        # Use LLM for SQL generation
        llm_result = await llm_agent.generate_sql_from_question(
            request.question,
            schema
        )

        sql = llm_result.get("sql", "")

        if not sql:
            # Fallback to pattern-based generation
            result = await query_agent.process_query(request.question)
        else:
            # Execute LLM-generated SQL
            query_result = await duckdb_tool.query(sql)

            if "error" in query_result:
                # Fallback to pattern-based
                result = await query_agent.process_query(request.question)
            else:
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
async def list_files():
    """List all generated files"""
    files = []

    # Check for any CSV files in data/raw using BASE_PATH
    raw_dir = os.path.join(BASE_PATH, "data/raw")
    if os.path.exists(raw_dir):
        for f in os.listdir(raw_dir):
            if f.endswith(('.csv', '.json', '.xlsx', '.xls')):
                full_path = os.path.join(raw_dir, f)
                files.append({
                    "name": f,
                    "path": f"data/raw/{f}",
                    "type": f.split('.')[-1],
                    "category": "raw_data",
                    "size": os.path.getsize(full_path),
                    "modified": datetime.fromtimestamp(os.path.getmtime(full_path)).isoformat()
                })

    # Check for any CSV files in data/clean
    clean_dir = os.path.join(BASE_PATH, "data/clean")
    if os.path.exists(clean_dir):
        for f in os.listdir(clean_dir):
            if f.endswith('.csv'):
                full_path = os.path.join(clean_dir, f)
                files.append({
                    "name": f,
                    "path": f"data/clean/{f}",
                    "type": "csv",
                    "category": "clean_data",
                    "size": os.path.getsize(full_path),
                    "modified": datetime.fromtimestamp(os.path.getmtime(full_path)).isoformat()
                })

    # ✅ FIX: Scan ALL files in pipelines directory (not just hardcoded names)
    pipelines_dir = os.path.join(BASE_PATH, "pipelines")
    if os.path.exists(pipelines_dir):
        for f in os.listdir(pipelines_dir):
            if f.endswith('.py'):
                full_path = os.path.join(pipelines_dir, f)
                files.append({
                    "name": f,
                    "path": f"pipelines/{f}",
                    "type": "py",
                    "category": "pipeline",
                    "size": os.path.getsize(full_path),
                    "modified": datetime.fromtimestamp(os.path.getmtime(full_path)).isoformat()
                })

    # ✅ FIX: Scan ALL files in reports directory (not just hardcoded names)
    reports_dir = os.path.join(BASE_PATH, "reports")
    if os.path.exists(reports_dir):
        for f in os.listdir(reports_dir):
            if f.endswith('.csv'):
                full_path = os.path.join(reports_dir, f)
                files.append({
                    "name": f,
                    "path": f"reports/{f}",
                    "type": "csv",
                    "category": "report",
                    "size": os.path.getsize(full_path),
                    "modified": datetime.fromtimestamp(os.path.getmtime(full_path)).isoformat()
                })

    # Add warehouse files
    warehouse_files = [
        ("warehouse/warehouse.duckdb", "warehouse", "duckdb"),
        ("warehouse/schema_cache.json", "schema", "json"),
    ]

    # Add dbt models using DBT_DIR from utils
    dbt_models_dir = os.path.join(DBT_DIR, "models")
    if os.path.exists(dbt_models_dir):
        for root, dirs, filenames in os.walk(dbt_models_dir):
            for f in filenames:
                if f.endswith('.sql') or f.endswith('.yml'):
                    rel_path = os.path.join(root, f).replace(BASE_PATH + '/', '')
                    full_path = os.path.join(root, f)
                    files.append({
                        "name": f,
                        "path": rel_path,
                        "type": f.split('.')[-1],
                        "category": "dbt_model",
                        "size": os.path.getsize(full_path),
                        "modified": datetime.fromtimestamp(os.path.getmtime(full_path)).isoformat()
                    })

    for path, category, file_type in warehouse_files:
        full_path = os.path.join(BASE_PATH, path)
        if os.path.exists(full_path):
            files.append({
                "name": os.path.basename(path),
                "path": path,
                "type": file_type,
                "category": category,
                "size": os.path.getsize(full_path),
                "modified": datetime.fromtimestamp(os.path.getmtime(full_path)).isoformat()
            })

    return {"files": files, "count": len(files)}


@app.get("/files/{file_path:path}")
async def get_file(file_path: str):
    """Get a specific file by path (safe - prevents path traversal)"""
    # Canonicalize and ensure the resolved path is within BASE_PATH
    full_path = os.path.realpath(os.path.join(BASE_PATH, file_path))
    if not full_path.startswith(os.path.realpath(BASE_PATH) + os.sep):
        raise HTTPException(status_code=403, detail="Access denied: path outside project directory")
    if not os.path.exists(full_path):
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
async def generate_pipeline_endpoint(file_path: str = None):
    """Generate a pipeline for the current or specified data file"""
    from agent.pipeline_generator import PipelineGenerator

    generator = PipelineGenerator()

    if file_path:
        result = await generator.generate_from_file(file_path)
    else:
        schema = schema_detector.load_schema_cache()
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
async def get_dashboard_stats():
    """Get dashboard statistics"""
    schema = schema_detector.load_schema_cache()

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
    if os.path.exists(WAREHOUSE_DB_PATH):
        try:
            con = duckdb.connect(WAREHOUSE_DB_PATH, read_only=True)
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
    if os.path.exists(REPORTS_DIR):
        stats["reports"] = len([f for f in os.listdir(REPORTS_DIR) if f.endswith('.csv')])

    return stats


@app.get("/dashboard/charts")
async def get_chart_data():
    """Get data for dashboard charts - Dynamic based on schema"""
    schema = schema_detector.load_schema_cache()

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

    if not schema or not os.path.exists(WAREHOUSE_DB_PATH):
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
        con = duckdb.connect(WAREHOUSE_DB_PATH, read_only=True)

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
async def list_warehouse_tables():
    """List all tables in the warehouse"""
    tables = []
    if os.path.exists(WAREHOUSE_DB_PATH):
        try:
            con = duckdb.connect(WAREHOUSE_DB_PATH, read_only=True)
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
async def get_table_data(table_name: str, limit: int = 100):
    """Get data from a specific table"""
    if not validate_identifier(table_name):
        raise HTTPException(status_code=400, detail=f"Invalid table name: {table_name!r}")

    if not os.path.exists(WAREHOUSE_DB_PATH):
        raise HTTPException(status_code=404, detail="Warehouse not found")

    try:
        con = duckdb.connect(WAREHOUSE_DB_PATH, read_only=True)

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
async def get_warehouse_sample(limit: int = 20):
    """Get sample data from the current table"""
    result = await duckdb_tool.get_sample_data(limit=limit)
    return result

@app.get("/download/{file_path:path}")
async def download_file(file_path: str):
    """Download a file with proper Content-Disposition header"""
    from fastapi.responses import FileResponse
    
    full_path = os.path.realpath(os.path.join(BASE_PATH, file_path))
    if not full_path.startswith(os.path.realpath(BASE_PATH) + os.sep):
        raise HTTPException(status_code=403, detail="Access denied: path outside project directory")
    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    filename = os.path.basename(full_path)
    return FileResponse(
        path=full_path,
        filename=filename,
        media_type="application/octet-stream"
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3001)
