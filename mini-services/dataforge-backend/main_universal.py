"""
DataForge AI - Universal FastAPI Backend
Supports ANY dataset type: Sales, News, Medical, Financial, Custom
"""

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import os
import sys
import tempfile
import shutil
from datetime import datetime

# Add paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import configuration and tools
from utils.config import (
    config_manager, DatasetConfig, DataType, ColumnConfig
)
from agent.tools.universal_duckdb_tool import UniversalDuckDBTool
from agent.tools.universal_query_agent import UniversalQueryAgent

# Initialize tools
duckdb_tool = UniversalDuckDBTool()
query_agent = UniversalQueryAgent()

# FastAPI app
app = FastAPI(
    title="DataForge AI - Universal Backend",
    description="AI-powered Data Engineering Platform - Works with ANY dataset",
    version="2.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# REQUEST/RESPONSE MODELS
# ============================================================================

class CommandRequest(BaseModel):
    command: str
    dataset: Optional[str] = None
    user_id: Optional[str] = None


class QueryRequest(BaseModel):
    question: str
    dataset: Optional[str] = None
    user_id: Optional[str] = None


class DatasetSelectRequest(BaseModel):
    dataset: str


class CustomDatasetRequest(BaseModel):
    name: str
    description: Optional[str] = ""
    csv_path: Optional[str] = None


# ============================================================================
# HEALTH & STATUS
# ============================================================================

@app.get("/")
async def root():
    return {
        "message": "DataForge AI Backend v2.0 - Universal Dataset Support",
        "version": "2.0.0",
        "supported_datasets": config_manager.list_datasets(),
        "current_dataset": config_manager.current_dataset
    }


@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


# ============================================================================
# DATASET MANAGEMENT
# ============================================================================

@app.get("/datasets")
async def list_datasets():
    """List all available dataset configurations"""
    datasets = []
    for name in config_manager.list_datasets():
        config = config_manager.get_config(name)
        datasets.append({
            "name": name,
            "type": config.data_type.value,
            "description": config.description,
            "column_count": len(config.columns),
            "query_patterns": len(config.query_patterns)
        })
    
    return {
        "datasets": datasets,
        "count": len(datasets),
        "current": config_manager.current_dataset
    }


@app.get("/datasets/{dataset_name}")
async def get_dataset_config(dataset_name: str):
    """Get configuration for a specific dataset"""
    config = config_manager.get_config(dataset_name)
    if not config:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    return {
        "name": config.name,
        "type": config.data_type.value,
        "description": config.description,
        "columns": [col.to_dict() for col in config.columns],
        "query_patterns": list(config.query_patterns.keys()),
        "category_columns": config.category_columns,
        "numeric_columns": config.numeric_columns,
        "date_column": config.date_column
    }


@app.post("/datasets/select")
async def select_dataset(request: DatasetSelectRequest):
    """Select the active dataset"""
    if config_manager.set_current_dataset(request.dataset):
        # Also update tools
        duckdb_tool.set_dataset(request.dataset)
        query_agent.set_dataset(request.dataset)
        
        return {
            "status": "success",
            "message": f"Dataset '{request.dataset}' is now active",
            "config": config_manager.get_config(request.dataset).to_dict()
        }
    raise HTTPException(status_code=404, detail="Dataset not found")


@app.post("/datasets/upload")
async def upload_dataset(
    file: UploadFile = File(...),
    name: Optional[str] = None,
    description: Optional[str] = ""
):
    """Upload a new dataset CSV and auto-detect schema"""
    
    # Save uploaded file
    if not name:
        name = file.filename.replace(".csv", "").replace(" ", "_")
    
    upload_dir = "/home/z/my-project/data/raw"
    os.makedirs(upload_dir, exist_ok=True)
    
    file_path = os.path.join(upload_dir, f"{name}.csv")
    
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Auto-detect schema
    config = config_manager.auto_detect_schema(file_path, name)
    config.description = description or f"Uploaded dataset: {name}"
    
    # Add to manager
    config_manager.add_custom_config(config)
    
    # Ingest the data
    await duckdb_tool.load_dataset(file_path, name)
    
    return {
        "status": "success",
        "message": f"Dataset '{name}' uploaded and processed",
        "schema": await duckdb_tool.discover_schema(file_path)
    }


# ============================================================================
# PIPELINE OPERATIONS
# ============================================================================

@app.post("/run-agent")
async def run_agent(request: CommandRequest):
    """
    Execute a natural language command.
    Uses the selected dataset configuration.
    """
    import traceback
    
    try:
        # Set dataset if specified
        if request.dataset:
            duckdb_tool.set_dataset(request.dataset)
            query_agent.set_dataset(request.dataset)
        
        config = duckdb_tool.get_config()
        if not config:
            return {
                "status": "error",
                "message": "No dataset selected. Use /datasets/select first."
            }
        
        command = request.command.lower()
        
        # Detect operation type
        if any(kw in command for kw in ["ingest", "load", "import"]):
            result = await duckdb_tool.ingest()
            return {
                "status": "success",
                "operation": "ingest",
                "dataset": config.name,
                "message": result,
                "table": f"{config.name}_raw"
            }
        
        elif any(kw in command for kw in ["transform", "clean", "process"]):
            result = await duckdb_tool.transform()
            return {
                "status": "success",
                "operation": "transform",
                "dataset": config.name,
                "message": result,
                "table": f"{config.name}_clean"
            }
        
        elif any(kw in command for kw in ["pipeline", "full", "run all"]):
            # Execute full pipeline
            logs = []
            
            ingest_result = await duckdb_tool.ingest()
            logs.append(f"[Ingest] {ingest_result}")
            
            transform_result = await duckdb_tool.transform()
            logs.append(f"[Transform] {transform_result}")
            
            return {
                "status": "success",
                "operation": "full_pipeline",
                "dataset": config.name,
                "logs": logs,
                "tables": [f"{config.name}_raw", f"{config.name}_clean"]
            }
        
        else:
            return {
                "status": "error",
                "message": "Could not determine operation. Try: 'load data', 'transform data', or 'run pipeline'"
            }
    
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# QUERY OPERATIONS
# ============================================================================

@app.post("/query")
async def execute_query(request: QueryRequest):
    """
    Execute a natural language query.
    Returns SQL and results based on the dataset schema.
    """
    try:
        # Set dataset if specified
        if request.dataset:
            query_agent.set_dataset(request.dataset)
        
        result = await query_agent.process_query(request.question)
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/query/suggestions")
async def get_query_suggestions(dataset: str = None):
    """Get suggested queries for the current dataset"""
    suggestions = await query_agent.suggest_queries(dataset)
    return {"suggestions": suggestions}


@app.post("/query/pattern/{pattern_name}")
async def execute_pattern(pattern_name: str, dataset: str = None, limit: int = 10):
    """Execute a named query pattern"""
    try:
        if dataset:
            query_agent.set_dataset(dataset)
        
        config = query_agent.get_config()
        if not config or pattern_name not in config.query_patterns:
            raise HTTPException(status_code=404, detail=f"Pattern '{pattern_name}' not found")
        
        pattern_sql = config.query_patterns[pattern_name]
        table = f"{config.name}_clean"
        sql = pattern_sql.format(table=table, limit=limit)
        
        result = await duckdb_tool.query(sql)
        
        return {
            "status": "success",
            "pattern": pattern_name,
            "sql": sql,
            "columns": result.get("columns", []),
            "data": result.get("data", []),
            "row_count": result.get("row_count", 0)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# FILE & WAREHOUSE OPERATIONS
# ============================================================================

@app.get("/files")
async def list_files():
    """List all generated files"""
    files = []
    base_path = "/home/z/my-project"
    
    # Check for each dataset
    for dataset_name in config_manager.list_datasets():
        config = config_manager.get_config(dataset_name)
        
        # Raw data
        raw_path = os.path.join(base_path, config.source_file)
        if os.path.exists(raw_path):
            files.append({
                "name": f"{dataset_name}.csv",
                "path": config.source_file,
                "type": "csv",
                "category": "raw",
                "dataset": dataset_name,
                "size": os.path.getsize(raw_path),
                "modified": datetime.fromtimestamp(os.path.getmtime(raw_path)).isoformat()
            })
        
        # Clean data
        clean_path = os.path.join(base_path, f"data/clean/{dataset_name}_clean.csv")
        if os.path.exists(clean_path):
            files.append({
                "name": f"{dataset_name}_clean.csv",
                "path": f"data/clean/{dataset_name}_clean.csv",
                "type": "csv",
                "category": "clean",
                "dataset": dataset_name,
                "size": os.path.getsize(clean_path),
                "modified": datetime.fromtimestamp(os.path.getmtime(clean_path)).isoformat()
            })
    
    # Warehouse
    warehouse_path = os.path.join(base_path, "warehouse/warehouse.duckdb")
    if os.path.exists(warehouse_path):
        files.append({
            "name": "warehouse.duckdb",
            "path": "warehouse/warehouse.duckdb",
            "type": "duckdb",
            "category": "warehouse",
            "size": os.path.getsize(warehouse_path),
            "modified": datetime.fromtimestamp(os.path.getmtime(warehouse_path)).isoformat()
        })
    
    return {"files": files, "count": len(files)}


@app.get("/warehouse/tables")
async def list_warehouse_tables():
    """List all tables in the warehouse"""
    tables = await duckdb_tool.list_tables()
    return {"tables": tables, "count": len(tables)}


@app.get("/warehouse/tables/{table_name}")
async def get_table_data(table_name: str, limit: int = 100):
    """Get data from a specific table"""
    sql = f"SELECT * FROM {table_name} LIMIT {limit}"
    result = await duckdb_tool.query(sql)
    
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    
    return {
        "table": table_name,
        "columns": result.get("columns", []),
        "data": result.get("data", []),
        "row_count": result.get("row_count", 0)
    }


# ============================================================================
# SCHEMA DISCOVERY
# ============================================================================

@app.post("/schema/discover")
async def discover_schema(csv_path: str):
    """Auto-discover schema from a CSV file"""
    full_path = os.path.join("/home/z/my-project", csv_path)
    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    schema = await duckdb_tool.discover_schema(full_path)
    return schema


# ============================================================================
# DASHBOARD
# ============================================================================

@app.get("/dashboard/stats")
async def get_dashboard_stats():
    """Get dashboard statistics"""
    tables = await duckdb_tool.list_tables()
    
    return {
        "total_datasets": len(config_manager.list_datasets()),
        "current_dataset": config_manager.current_dataset,
        "tables": len(tables),
        "success_rate": 95.5
    }


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3001)
