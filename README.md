<div align="center">

# Omnix AI

### LLM-Powered Data Pipeline Platform

**Build, Analyze, and Transform Data Pipelines with AI — All Through a Natural Language Interface**

<p>
  <img src="https://img.shields.io/badge/Next.js-16-black?logo=next.js" alt="Next.js 16" />
  <img src="https://img.shields.io/badge/TypeScript-5.7-3178c6?logo=typescript&logoColor=white" alt="TypeScript 5" />
  <img src="https://img.shields.io/badge/Tailwind_CSS-4-38bdf8?logo=tailwindcss" alt="Tailwind CSS 4" />
  <img src="https://img.shields.io/badge/shadcn%2Fui-New_York-18181b?logo=shadcnui" alt="shadcn/ui" />
  <img src="https://img.shields.io/badge/LangChain-0.3-1c3c3c?logo=chainlink" alt="LangChain" />
  <img src="https://img.shields.io/badge/LangGraph-0.2-19c37d?logo=graph&logoColor=white" alt="LangGraph" />
  <img src="https://img.shields.io/badge/DuckDB-0.10-ffc107?logo=duckdb&logoColor=black" alt="DuckDB" />
  <img src="https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white" alt="FastAPI" />
  <img src="https://img.shields.io/badge/Python-3.12-3776ab?logo=python&logoColor=white" alt="Python 3.12" />
</p>

<p>
  <img src="https://img.shields.io/badge/11_Agent_Nodes-8b5cf6" alt="11 Agent Nodes" />
  <img src="https://img.shields.io/badge/7_Specialized_Tools-f59e0b" alt="7 Tools" />
  <img src="https://img.shields.io/badge/38_API_Endpoints-06b6d4" alt="38 API Endpoints" />
</p>

</div>

---

## Table of Contents

- [1. Project Title & Description](#1-project-title--description)
- [2. Architecture Diagram](#2-architecture-diagram)
- [3. Full Tech Stack](#3-full-tech-stack)
- [4. Complete Agents & Tools Reference](#4-complete-agents--tools-reference)
- [5. LangGraph Workflow Diagram](#5-langgraph-workflow-diagram)
- [6. API Routes Reference](#6-api-routes-reference)
- [7. How to Run Locally](#7-how-to-run-locally)
- [8. Environment Variables](#8-environment-variables)
- [9. Deployment Guide](#9-deployment-guide)
- [10. Project Structure](#10-project-structure)

---

## 1. Project Title & Description

**Omnix AI** is a full-stack, production-ready intelligent data pipeline platform that leverages a multi-agent AI architecture powered by **LangGraph** and **LangChain** to automate every stage of the data engineering lifecycle. Upload any dataset and the AI handles the rest.

### What It Does

Upload **any dataset** (CSV, JSON, XLSX) — sales, medical, finance, news, HR, IoT, or custom data — and the platform automatically:

1. **Detects** the dataset category and schema via heuristic + AI classification
2. **Analyzes** data quality, column semantics, and provides AI-powered business insights
3. **Generates** production-ready **dbt** transformation models (staging → intermediate → mart)
4. **Creates** automated ETL pipelines with step-by-step execution tracking
5. **Enables** natural language querying — ask questions in English, get SQL results instantly
6. **Connects** to 300+ external data sources via **Airbyte** integration
7. **Visualizes** data with interactive charts, stats cards, and pipeline DAGs

### Key Capabilities

| Capability | Description |
|---|---|
| **NL to SQL** | Ask "What are the top 10 products by revenue?" and get DuckDB results |
| **Auto Schema Detection** | Upload a CSV and the AI classifies it as sales, medical, finance, etc. |
| **dbt Generation** | AI generates staging, intermediate, and mart SQL models |
| **Pipeline Orchestration** | Multi-step ETL: ingest → clean → transform → load → report |
| **LangGraph Workflow** | Stateful agent orchestration with conditional routing and error recovery |

---

## 2. Architecture Diagram

Omnix AI follows a dual-service architecture: a **Next.js 16** frontend with API routes that proxy to a **FastAPI** Python backend housing the AI agent system.

```
┌──────────────────────────────────────────────────────────────────┐
│                     CLIENT LAYER (Browser)                        │
│  Landing Page · Auth (JWT) · Dashboard · Agent Workspace         │
│  File Explorer · Pipeline DAG · NL Query Box · dbt Models        │
└──────────────────────────┬───────────────────────────────────────┘
                           │  HTTP (port 3000)
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                  NEXT.JS 16 — FRONTEND + API GATEWAY             │
│  ┌──────────────────────┐  ┌─────────────────────────────────┐  │
│  │ React App (TypeScript)│  │ 18 API Routes (src/app/api/)   │  │
│  │ · Landing Page        │  │ · /auth    · /upload  · /files  │  │
│  │ · Dashboard           │  │ · /schema  · /query   · /agent  │  │
│  │ · Agent Workspace     │  │ · /llm/*   · /warehouse         │  │
│  │ · File Explorer       │  │ · /pipelines · /dashboard       │  │
│  │ · Pipeline DAG        │  │ · /airbyte/* · /load-sample     │  │
│  └──────────────────────┘  └────────────┬────────────────────┘  │
│                                           │ proxy (port 3030)     │
└───────────────────────────────────────────┼───────────────────────┘
                                            │
                                            ▼
┌──────────────────────────────────────────────────────────────────┐
│               FASTAPI PYTHON BACKEND (port 3030)                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │              LANGGRAPH AGENT ORCHESTRATION                  │ │
│  │  START → classify_intent → load_schema → [conditional]     │ │
│  │  → action_node (analyze|query|dbt|pipeline|report) → END   │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Agents: Master · LLM · Query · Pipeline Generator · Schema Det.  │
│  Tools:  DuckDB · dbt · Report · XLSX · Airbyte Connector/Tool   │
└───────────────────────────┬──────────────────┬───────────────────┘
                            │                  │
               ┌────────────┘                  └────────────┐
               ▼                                            ▼
┌──────────────────────────┐          ┌──────────────────────────┐
│  DUCKDB DATA WAREHOUSE   │          │  PRISMA + SQLITE         │
│  · Raw Data Tables       │          │  · Users (auth)          │
│  · Cleaned Data          │          │  · Posts                 │
│  · Schema Cache          │          │  · JWT Sessions          │
│  · Query Results         │          │                          │
└──────────────────────────┘          └──────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────┐
│                    EXTERNAL INTEGRATIONS                          │
│  · LLM (OpenAI via LangChain ChatOpenAI)                        │
│  · Airbyte (300+ data source connectors)                         │
│  · dbt Core (SQL transformation models)                          │
└──────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Upload CSV/JSON/XLSX
       │
       ▼
  ┌─────────┐    ┌──────────────┐    ┌─────────────┐
  │  Next.js ├───►│  FastAPI     ├───►│  Schema     │
  │  /upload │    │  /ingest     │    │  Detector   │
  └─────────┘    └──────┬───────┘    └──────┬──────┘
                        │                    │
                        ▼                    ▼
                 ┌─────────────┐     ┌──────────────┐
                 │  DuckDB     │     │  LangGraph   │
                 │  Warehouse  │     │  Orchestrate │
                 └──────┬──────┘     └──────┬───────┘
                        │                   │
            ┌───────────┼───────────┐       │
            ▼           ▼           ▼       ▼
       ┌────────┐ ┌─────────┐ ┌────────┐ ┌────────┐
       │ Analyze│ │ NL→SQL  │ │  dbt   │ │Pipeline│
       │  LLM   │ │  LLM    │ │  Gen   │ │  Exec  │
       └────────┘ └─────────┘ └────────┘ └────────┘
            │           │           │           │
            └───────────┴───────────┴───────────┘
                        │
                        ▼
                  ┌───────────┐
                  │  Results  │
                  │  + Reports│
                  └───────────┘
```

---

## 3. Full Tech Stack

### Frontend

| Technology | Version | Purpose |
|---|---|---|
| **Next.js** | 16.1 | React framework with App Router, API routes, and SSR |
| **TypeScript** | 5.7 | Type-safe development with strict mode |
| **React** | 19.0 | UI library with concurrent features |
| **Tailwind CSS** | 4 | Utility-first CSS framework (JIT compilation) |
| **shadcn/ui** | New York | 50+ production-grade UI components (Radix primitives) |
| **Lucide React** | 0.525 | Consistent icon library (200+ icons) |
| **next-themes** | 0.4 | Dark/Light mode with system preference detection |
| **Zustand** | 5.0 | Lightweight client state management (auth store) |
| **Framer Motion** | 12.23 | Smooth animations and page transitions |
| **Recharts** | 2.15 | Responsive chart library (bar, line, pie, area) |
| **TanStack Query** | 5.82 | Server state management with caching |
| **TanStack Table** | 8.21 | Headless table primitives |
| **react-hook-form** | 7.60 | Performant form state management |
| **zod** | 4.0 | TypeScript-first schema validation |
| **sonner** | 2.0 | Toast notification system |
| **react-markdown** | 10.1 | Markdown rendering for AI responses |
| **react-syntax-highlighter** | 15.6 | Code block syntax highlighting |
| **bcryptjs** | 3.0 | Secure password hashing (server-side) |
| **jsonwebtoken** | 9.0 | JWT token generation and verification |
| **Prisma** | 6.11 | Type-safe ORM for SQLite (auth/database) |

### Backend

| Technology | Version | Purpose |
|---|---|---|
| **FastAPI** | 0.115 | High-performance async Python web framework |
| **Python** | 3.12 | Runtime with modern type hints and performance |
| **Uvicorn** | 0.27 | ASGI server for FastAPI |
| **LangChain** | 0.3+ | LLM orchestration framework |
| **LangChain-OpenAI** | 0.3+ | OpenAI-compatible LLM provider integration |
| **LangGraph** | 0.2+ | Stateful multi-step agent workflow engine |
| **Pydantic** | 2.6+ | Data validation and structured LLM output schemas |
| **DuckDB** | 0.10 | In-process analytical database (OLAP warehouse) |
| **Pandas** | 2.2 | Data manipulation and processing |
| **httpx** | 0.26+ | Async HTTP client for LLM API and Airbyte calls |
| **OpenAI** | 1.12+ | OpenAI API client (used via LangChain) |

### Database

| Technology | Purpose |
|---|---|
| **DuckDB** | In-process analytical database for data warehouse (raw + clean tables, schema cache, query results) |
| **Prisma + SQLite** | Type-safe ORM for application data (users, posts, JWT sessions) |

### AI

| Technology | Purpose |
|---|---|
| **LangChain ChatOpenAI** | 3 temperature-tuned LLM instances for analysis (0.3), SQL (0.0), and general tasks (0.7) |
| **Structured Output** | `with_structured_output()` using Pydantic models for typed, validated LLM responses |
| **Session Memory** | 20-message rolling conversation memory per session for contextual follow-ups |
| **Graceful Fallback** | Pattern-based heuristic generators when LLM is unavailable — never fabricates results |

### Auth

| Technology | Purpose |
|---|---|
| **jsonwebtoken** | JWT token generation and verification (7-day expiry) |
| **bcryptjs** | Secure password hashing with 10 salt rounds |

### Integration

| Technology | Purpose |
|---|---|
| **Airbyte** | 300+ data source connectors (PostgreSQL, MySQL, MongoDB, S3, BigQuery, Salesforce, Stripe) |
| **dbt Core** | Production-quality SQL transformation models (staging, intermediate, mart) |
| **openpyxl** | XLSX file reading and processing |
| **xlrd** | Legacy XLS file reading |
| **python-multipart** | File upload handling in FastAPI |

---

## 4. Complete Agents & Tools Reference

### Orchestration Layer

#### LangGraph StateGraph — `agent/orchestration.py`

The central workflow engine. Defines a compiled `StateGraph` with 11 nodes, conditional routing based on 8 intent categories, and error recovery.

| Property | Detail |
|---|---|
| **File** | `mini-services/dataforge-backend/agent/orchestration.py` |
| **State** | `AgentState` TypedDict with 19 fields (command, session_id, intent, schema, analysis_result, sql_result, dbt_result, pipeline_steps, etc.) |
| **Nodes** | 11: `classify_intent`, `load_schema`, `ingest`, `analyze`, `query`, `generate_dbt`, `pipeline`, `report`, `schema_info`, `help`, `error` |
| **Routing** | `route_by_intent()` — conditional edge that checks preconditions (e.g., requires schema for analyze/query/dbt/report) |
| **Intents** | 8: ingest, analyze, query, generate_dbt, pipeline, report, schema, help |
| **Entry** | `START → classify_intent → load_schema → [conditional] → action → END` |
| **Singleton** | Compiled graph is a module-level singleton: `graph = build_graph()` |
| **Key Methods** | `classify_intent()`, `load_current_schema()`, `route_by_intent()`, `build_graph()` |
| **How it Works** | Receives `AgentState` → classifies command intent via keyword matching → loads schema from cache → routes to appropriate action node → each node updates state and returns partial dict → terminal nodes connect to `END` |

---

### Agents

#### 1. MasterAgent — `agent/master_agent.py`

The top-level orchestrator. Wraps the compiled LangGraph graph and provides the public API for `main.py`.

| Property | Detail |
|---|---|
| **File** | `mini-services/dataforge-backend/agent/master_agent.py` |
| **Purpose** | Command orchestrator — routes natural language commands to specialized agents via LangGraph |
| **State** | Tracks `execution_count`, `pipelines` (history list), `last_execution` |
| **Methods** | `await execute(command, session_id)`, `await execute_pipeline(command)`, `get_stats()` |
| **How it Works** | Receives user command → builds initial `AgentState` with all 19 fields → invokes compiled LangGraph `graph.ainvoke()` → formats result using `format_result()` from utils → appends to `pipelines` history → tracks `execution_count` and `last_execution` |

**`execute(command, session_id="default")`** — Async. The primary entry point. Creates initial state, runs through the graph, returns standardized result dict.

**`execute_pipeline(command)`** — Async. Accepts a pipeline_id string (e.g., `pipeline_3`) or a fresh command. If a matching pipeline is found, re-executes the original command.

**`get_stats()`** — Sync. Returns execution count, last 10 pipeline records, and current schema.

---

#### 2. LLMAgent — `agent/tools/llm_agent.py`

Deep AI-powered analysis using LangChain `ChatOpenAI` with structured output.

| Property | Detail |
|---|---|
| **File** | `mini-services/dataforge-backend/agent/tools/llm_agent.py` |
| **Purpose** | LLM-powered dataset analysis, NL→SQL generation, dbt model generation, and pipeline code generation |
| **LLM Instances** | 3 `ChatOpenAI` instances: `llm_sql` (temp=0.0), `llm_analysis` (temp=0.3), `llm_general` (temp=0.7) |
| **Configuration** | Model: `gpt-4o-mini` (configurable via `LLM_MODEL`), `max_retries=3`, `timeout=60` |
| **Session Memory** | `_sessions` dict, capped at 20 messages per session via `_push_session()` / `_session_messages()` |
| **Methods** | `analyze_dataset(schema_info, sample_data, session_id)`, `generate_sql(question, schema_info, session_id)`, `generate_sql_from_question(question, schema_info, analysis)`, `generate_dbt_models(schema_info, analysis, session_id)`, `generate_pipeline_code(schema_info, analysis, operations)`, `check_health()` |
| **Structured Output** | `with_structured_output(DatasetAnalysis)` for analysis, `with_structured_output(SQLResult)` for SQL, `with_structured_output(DBTModelsOutput)` for dbt |
| **Fallback** | 4 fallback methods: `_generate_fallback_sql()`, `_generate_fallback_analysis()`, `_generate_fallback_dbt()`, `_generate_fallback_pipeline()` |
| **How it Works** | If `OPENAI_API_KEY` is set: builds system prompt with schema context → pushes to session memory → invokes LLM chain with `with_structured_output()` → validates SQL via `validate_sql()` → returns typed result. If unavailable: returns heuristic-based fallback with `llm_used: False` and `confidence_score: 0.0` |

**`check_health()`** — Returns dict with `available`, `model`, `api_base`, `api_key_set`, `sessions_active`.

---

#### 3. QueryAgent — `agent/query_agent.py`

Natural language to SQL conversion and execution with pattern matching + LLM fallback.

| Property | Detail |
|---|---|
| **File** | `mini-services/dataforge-backend/agent/query_agent.py` |
| **Purpose** | NL→SQL conversion and execution on DuckDB warehouse |
| **Methods** | `process_query(question)`, `get_suggested_queries()`, `get_current_schema()`, `generate_sql(question)`, `execute_sql(sql)` |
| **How it Works** | Takes NL question → refreshes schema from cache → uses `categorize_columns()` to classify columns into numeric/categorical/date/id → dynamically detects ID column via `_find_id_column()` (no hardcoded references) → matches 12+ SQL patterns (top-N, bottom-N, trend, group-by, total, average, count, compare, distribution, dataset-specific) → generates SQL with `quote_identifier()` → validates via `validate_sql()` → executes on DuckDB with 30s timeout → returns formatted results with execution time |

---

#### 4. PipelineGenerator — `agent/pipeline_generator.py`

Automated ETL pipeline code generation.

| Property | Detail |
|---|---|
| **File** | `mini-services/dataforge-backend/agent/pipeline_generator.py` |
| **Purpose** | Generates executable Python pipeline code dynamically for ANY dataset |
| **Methods** | `generate(operations, schema)`, `generate_from_file(file_path, operations)` |
| **How it Works** | Loads schema from cache or file → generates dynamic `ingest()`, `transform()`, and `generate_report()` methods based on column types and semantics → assembles them into a `DataPipeline` class → writes to `pipelines/pipeline.py` → returns the generated code string |
| **Path Import** | Uses `BASE_PATH` from `agent.utils` (not hardcoded) |

---

### Tools

#### 5. SchemaDetector — `agent/tools/schema_detector.py`

| Property | Detail |
|---|---|
| **File** | `mini-services/dataforge-backend/agent/tools/schema_detector.py` |
| **Purpose** | Automatic dataset category and column semantic detection |
| **Methods** | `load_schema_cache()`, `detect_schema_from_file(file_path)`, `get_transformation_sql()`, `generate_sql_suggestions()` |
| **How it Works** | Reads CSV/JSON headers and sample data → applies heuristic rules for column classification (semantic types: id, name, category, location, money, count, score, percentage, datetime, generic) → uses `categorize_columns()` from utils for type inference → saves schema via `save_schema()` (thread-safe) → outputs structured schema with column types, nullability, and sample values |

---

#### 6. DuckDBTool — `agent/tools/duckdb_tool.py`

| Property | Detail |
|---|---|
| **File** | `mini-services/dataforge-backend/agent/tools/duckdb_tool.py` |
| **Purpose** | Core warehouse operations: ingest, transform, query, create tables, export |
| **Methods** | `ingest_file(file_path, table_name)`, `ingest()`, `transform()`, `query(sql)`, `get_schema(table_name)`, `get_current_schema()`, `create_table(table_name, schema)`, `export_table(table_name, output_path)`, `list_tables()`, `get_sample_data(table_name, limit)` |
| **Security** | All SQL uses `quote_identifier()` for injection prevention, `validate_identifier()` on all parameters, `validate_sql()` before execution, `safe_table_name()` for filenames, 30s query timeout |
| **How it Works** | Detects file format (CSV/JSON) → reads via Pandas → creates raw table in DuckDB using `safe_table_name()` → validates and quotes all identifiers → applies transformation SQL → exports clean data to CSV → updates schema cache |

---

#### 7. DBTTool — `agent/tools/dbt_tool.py`

| Property | Detail |
|---|---|
| **File** | `mini-services/dataforge-backend/agent/tools/dbt_tool.py` |
| **Purpose** | dbt model management: generate staging/intermediate/mart models |
| **Methods** | `generate_models(schema, analysis)` and related methods |
| **How it Works** | Receives schema and analysis → generates staging (cleaning), intermediate (aggregation), and mart (analytics) SQL models → writes to `DBT_DIR/models/` directory → includes `schema.yml` with tests and documentation |

---

#### 8. ReportTool — `agent/tools/report_tool.py`

| Property | Detail |
|---|---|
| **File** | `mini-services/dataforge-backend/agent/tools/report_tool.py` |
| **Purpose** | Report generation with data quality summaries and statistics |
| **Methods** | `generate()`, `generate_summary()`, `generate_chart_data(chart_type)` |
| **How it Works** | Loads schema → categorizes columns → builds dynamic aggregation query (group by first category, aggregate first numeric) → validates SQL → executes on DuckDB → exports to CSV at `REPORTS_DIR/report.csv` → supports summary generation and chart data extraction |

---

#### 9. XLSXProcessor — `agent/tools/xlsx_processor.py`

| Property | Detail |
|---|---|
| **File** | `mini-services/dataforge-backend/agent/tools/xlsx_processor.py` |
| **Purpose** | Excel file processing with multi-sheet support |
| **Methods** | `validate_file(file_path)`, `get_sheet_names(file_path)`, `get_sheet_preview(file_path, sheet_name, rows)`, `process_sheet(file_path, sheet_name, skip_rows, use_columns)`, `process_all_sheets(file_path)`, `to_csv(file_path, output_dir, sheet_name, combine_sheets)`, `to_json(file_path, output_dir, sheet_name, orient)`, `get_schema_info(file_path, sheet_name)`, `process_upload(file_content, filename, output_format)` |
| **How it Works** | Validates file extension and size (max 50MB) → reads via Pandas `ExcelFile` → processes individual or all sheets → cleans column names → detects semantic types → converts to CSV/JSON → extracts schema with column stats (type, nullable, unique count, sample values) |

---

#### 10. AirbyteConnector — `agent/tools/airbyte_connector.py`

| Property | Detail |
|---|---|
| **File** | `mini-services/dataforge-backend/agent/tools/airbyte_connector.py` |
| **Purpose** | Low-level Airbyte API client for HTTP requests to Airbyte server |
| **Methods** | `initialize()`, `list_source_definitions()`, `create_source(name, source_definition_id, connection_config)`, `test_source_connection(source_id)`, `discover_source_schema(source_id)`, `list_sources()`, `create_destination(name, destination_definition_id, connection_config)`, `list_destinations()`, `create_connection(name, source_id, destination_id, streams, schedule)`, `list_connections()`, `sync_connection(connection_id)`, `get_job_status(job_id)`, `get_connection_info(connection_id)`, `get_connection_template(source_type)` |
| **Helper Methods** | `create_postgres_source()`, `create_mysql_source()`, `create_s3_source()`, `create_duckdb_destination()` |
| **How it Works** | Uses `httpx.AsyncClient` with basic auth → makes authenticated requests to Airbyte `/api/v1/` endpoints → returns parsed JSON responses → provides popular connector fallback list when Airbyte is unavailable → supports demo mode for all CRUD operations |

---

#### 11. AirbyteTool — `agent/tools/airbyte_tool.py`

| Property | Detail |
|---|---|
| **File** | `mini-services/dataforge-backend/agent/tools/airbyte_tool.py` |
| **Purpose** | Higher-level Airbyte integration: source management, connection setup, sync scheduling |
| **Methods** | `trigger_sync(connection_id)`, `get_connection_status(connection_id)`, `list_connections()`, `create_connection(config)` |
| **How it Works** | Simplified interface over `AirbyteConnector` with simulated demo responses for standalone operation |

---

### Shared Modules

#### `agent/utils.py` — Centralized Shared Utilities

All agents and tools import from this single source of truth for paths, schema management, and SQL safety.

| Function / Constant | Type | Description |
|---|---|---|
| `BASE_PATH` | `str` | Project root, parameterized via `DATAFORGE_BASE_PATH` env var |
| `DATA_DIR` / `RAW_DATA_DIR` / `CLEAN_DATA_DIR` / `SAMPLES_DIR` | `str` | Data directory paths |
| `WAREHOUSE_DIR` / `SCHEMA_CACHE_PATH` / `WAREHOUSE_DB_PATH` | `str` | Warehouse paths |
| `PIPELINES_DIR` / `REPORTS_DIR` / `DBT_DIR` | `str` | Output paths |
| `QUERY_TIMEOUT` | `int` | 30-second query timeout constant |
| `load_schema()` | `func` | Thread-safe schema loading with `threading.Lock` |
| `save_schema()` | `func` | Thread-safe schema writing with `threading.Lock` |
| `validate_identifier()` | `func` | Regex validation: `^[a-zA-Z_][a-zA-Z0-9_]{0,127}$` |
| `quote_identifier()` | `func` | Double-quote escaping for DuckDB identifiers |
| `safe_table_name()` | `func` | Sanitizes filenames into safe SQL table names |
| `validate_sql()` | `func` | Blocks DROP TABLE, DELETE, TRUNCATE, ALTER, GRANT, REVOKE, ATTACH, COPY FROM URL, multi-statement |
| `categorize_columns()` | `func` | Classifies columns into: numeric, categorical, date, text, id |
| `format_result()` | `func` | Standardized result dict for all agent operations |

#### `agent/schemas.py` — Pydantic Output Models

All LLM structured outputs use these validated Pydantic models with `with_structured_output()`:

| Model | Fields | Purpose |
|---|---|---|
| `ColumnAnalysis` | `semantic_type`, `business_meaning`, `data_quality`, `issues`, `recommendations` | Per-column analysis result |
| `DataQualitySummary` | `overall_score` (0–1), `issues`, `recommendations` | Dataset-wide quality assessment |
| `RecommendedTransformation` | `type`, `description`, `sql_template` | AI-suggested data transformation |
| `SuggestedMetric` | `name`, `description`, `formula`, `business_value` | Business KPI recommendation |
| `DatasetAnalysis` | `dataset_type`, `confidence_score`, `column_analysis`, `data_quality_summary`, `transformations`, `metrics`, `insights` | Complete dataset analysis output |
| `SQLResult` | `sql`, `explanation`, `generated_by` ("llm" or "fallback") | NL→SQL generation result |
| `DBTModel` | `path`, `content`, `description` | Single dbt model file |
| `DBTModelsOutput` | `models` (list of `DBTModel`) | dbt generation result |
| `PipelineStep` | `name`, `operation`, `description` | Single pipeline step |
| `PipelineOutput` | `steps`, `description` | Pipeline generation result |

---

## 5. LangGraph Workflow Diagram

```
START
  │
  ▼
classify_intent ───── Keyword matching against 8 intent categories
  │                     (ingest, analyze, query, generate_dbt,
  │                      pipeline, report, schema, help)
  ▼
load_schema ──────── Loads schema from thread-safe cache
  │
  ▼
route_by_intent ──── Conditional router (checks preconditions)
  │
  ├── ingest ────────── Load most recent file from data/raw/ into DuckDB
  │                      → detect schema → ingest → transform
  │
  ├── analyze ────────── LLM-powered dataset analysis (requires schema)
  │                      → ChatOpenAI(temp=0.3) + structured output
  │
  ├── query ──────────── NL → SQL generation + execution (requires schema)
  │                      → ChatOpenAI(temp=0.0) → validate SQL → execute
  │
  ├── generate_dbt ──── AI dbt model generation (requires schema)
  │                      → ChatOpenAI(temp=0.7) → staging/intermediate/mart
  │
  ├── pipeline ───────── Full ETL: ingest → transform → report
  │                      → generates pipeline code + executes steps
  │
  ├── report ─────────── Generate CSV summary report
  │
  ├── schema_info ────── Display current schema details
  │
  ├── help ───────────── List available commands with examples
  │
  └── error ──────────── Friendly error handler (fallback for missing schema)
  │
  ▼
END
```

**Supported Intents & Keywords:**

| Intent | Keywords |
|---|---|
| `ingest` | upload, ingest, load, import |
| `analyze` | analyze, analysis, inspect, examine, what is |
| `query` | query, show, select, find, top, average, sum, count, how many, trend, total, best, worst, compare, by region, monthly, yearly |
| `generate_dbt` | dbt, generate dbt, create models, transform models |
| `pipeline` | pipeline, etl, process, run pipeline, build pipeline |
| `report` | report, summary, export, download |
| `schema` | schema, columns, structure, describe |
| `help` | help, what can you do, commands, list |

---

## 6. API Routes Reference

### Frontend API Routes (Next.js — port 3000)

All routes in `src/app/api/` proxy to the Python backend (port 3030) with fallback responses when the backend is unavailable.

#### Authentication

| # | Method | Endpoint | Description | Request |
|---|---|---|---|---|
| 1 | `POST` | `/api/auth` | Login, Signup, or Verify JWT | `{ action: "login"\|"signup"\|"verify", email, password, name? }` |
| 2 | `GET` | `/api/auth` | Auth health check | — |

#### File Operations

| # | Method | Endpoint | Description | Request |
|---|---|---|---|---|
| 3 | `POST` | `/api/upload` | Upload CSV/JSON/XLSX file | `multipart/form-data` with file |
| 4 | `GET` | `/api/files` | List all project files | — |
| 5 | `POST` | `/api/load-sample` | Load a sample dataset | Query param: `?file={name}` |

#### Data Analysis & AI

| # | Method | Endpoint | Description | Request |
|---|---|---|---|---|
| 6 | `GET` | `/api/schema` | Get current dataset schema | — |
| 7 | `POST` | `/api/schema` | Update dataset schema | Schema object |
| 8 | `POST` | `/api/llm/analyze` | Run LLM-powered dataset analysis | `{ schema, sample_data? }` |
| 9 | `POST` | `/api/llm/generate-dbt` | Generate dbt transformation models | `{ schema, analysis? }` |
| 10 | `POST` | `/api/query` | Execute NL→SQL query | `{ question, schema? }` |

#### Dashboard & Visualization

| # | Method | Endpoint | Description | Request |
|---|---|---|---|---|
| 11 | `GET` | `/api/dashboard` | Dashboard statistics | — |
| 12 | `GET` | `/api/dashboard/charts` | Chart visualization data | — |

#### Agent & Pipelines

| # | Method | Endpoint | Description | Request |
|---|---|---|---|---|
| 13 | `POST` | `/api/agent` | Execute agent command via LangGraph | `{ command, session_id? }` |
| 14 | `GET` | `/api/agent` | Get agent status | — |
| 15 | `GET` | `/api/pipelines` | Get pipeline history | — |
| 16 | `POST` | `/api/pipelines` | Create or execute pipeline | Pipeline config |

#### Warehouse

| # | Method | Endpoint | Description | Request |
|---|---|---|---|---|
| 17 | `GET` | `/api/warehouse` | Warehouse info | — |

#### Airbyte Integration

| # | Method | Endpoint | Description | Request |
|---|---|---|---|---|
| 18 | `GET` | `/api/airbyte/source-definitions` | List source types (300+) | — |
| 19 | `GET` | `/api/airbyte/sources` | List configured sources | — |
| 20 | `POST` | `/api/airbyte/sources` | Create new source | Source config |
| 21 | `GET` | `/api/airbyte/connections` | List connections | — |
| 22 | `POST` | `/api/airbyte/connections` | Create connection | Connection config |
| 23 | `GET` | `/api/airbyte/templates/{type}` | Get connection template | — |

---

### Python Backend API (FastAPI — port 3030)

The FastAPI backend exposes 38 endpoints. Key endpoints beyond what the frontend proxies:

| # | Method | Endpoint | Description |
|---|---|---|---|
| 1 | `GET` | `/` | Root — returns version and status |
| 2 | `GET` | `/health` | Backend health check |
| 3 | `GET` | `/schema` | Get current detected schema |
| 4 | `POST` | `/schema/detect` | Detect schema from a specific file |
| 5 | `GET` | `/schema/suggestions` | Get suggested queries based on schema |
| 6 | `POST` | `/upload` | Upload a data file (CSV, JSON, XLSX) |
| 7 | `POST` | `/upload-and-process` | Upload and fully process with LLM analysis |
| 8 | `POST` | `/xlsx/upload` | Upload XLSX with multi-sheet support |
| 9 | `GET` | `/xlsx/sheets/{file_path:path}` | Get all sheets from an Excel file |
| 10 | `GET` | `/xlsx/preview/{file_path:path}` | Preview data from an Excel sheet |
| 11 | `POST` | `/llm/analyze` | Run LLM-powered dataset analysis |
| 12 | `POST` | `/llm/generate-dbt` | Generate dbt models via LLM |
| 13 | `POST` | `/llm/generate-pipeline` | Generate Prefect pipeline via LLM |
| 14 | `GET` | `/airbyte/sources` | List all Airbyte sources |
| 15 | `GET` | `/airbyte/source-definitions` | List available source connector types |
| 16 | `POST` | `/airbyte/sources` | Create a new Airbyte source |
| 17 | `POST` | `/airbyte/sources/{source_id}/test` | Test source connection |
| 18 | `POST` | `/airbyte/sources/{source_id}/discover` | Discover source schema |
| 19 | `GET` | `/airbyte/destinations` | List all destinations |
| 20 | `POST` | `/airbyte/connections` | Create a connection |
| 21 | `GET` | `/airbyte/connections` | List all connections |
| 22 | `POST` | `/airbyte/connections/{id}/sync` | Trigger a sync |
| 23 | `GET` | `/airbyte/connections/{id}/status` | Get connection status |
| 24 | `GET` | `/airbyte/jobs/{job_id}` | Get sync job status |
| 25 | `GET` | `/airbyte/templates/{source_type}` | Get connection template |
| 26 | `POST` | `/run-agent` | Execute a command through master agent |
| 27 | `GET` | `/status` | Get system status |
| 28 | `POST` | `/query` | Execute NL→SQL query |
| 29 | `GET` | `/files` | List all generated files |
| 30 | `GET` | `/files/{file_path:path}` | Get a specific file (path traversal protected) |
| 31 | `GET` | `/pipelines` | List all pipelines |
| 32 | `POST` | `/pipelines/execute` | Execute a specific pipeline |
| 33 | `POST` | `/pipelines/generate` | Generate a pipeline for current data |
| 34 | `GET` | `/dashboard/stats` | Dashboard statistics with LLM health |
| 35 | `GET` | `/dashboard/charts` | Chart data (pipeline runs, distribution, trends) |
| 36 | `GET` | `/warehouse/tables` | List all warehouse tables |
| 37 | `GET` | `/warehouse/tables/{table_name}` | Get table data with column details |
| 38 | `GET` | `/warehouse/sample` | Get sample data from current table |

---

## 7. How to Run Locally

### Prerequisites

- **Node.js** 18+ or **Bun** runtime
- **Python** 3.12+
- **Git**

### Step-by-Step Setup

```bash
# 1. Install Node.js dependencies
bun install

# 2. Set up database
bun run db:push

# 3. Install Python backend
cd mini-services/dataforge-backend
pip install -r requirements.txt
cd ../..

# 4. Set environment variables
# Create .env file (see Environment Variables section below)

# 5. Start backend service
cd mini-services/dataforge-backend
bun run dev
# or: uvicorn main:app --host 0.0.0.0 --port 3030 --reload
cd ../..

# 6. Start frontend (already running on port 3000)
bun run dev
```

The application will be available at **http://localhost:3000**.

> **Note:** The Next.js frontend includes fallback responses, so the app is fully functional even without the Python backend running. AI analysis and NL→SQL features require the Python backend with an LLM API key.

### Loading Sample Data

After starting the app, click **"Load Sample Dataset"** in the Dashboard tab, or use the API:

```bash
curl -X POST "http://localhost:3000/api/load-sample?file=finance_stocks.csv"
```

Available sample datasets:
- `finance_stocks.csv` — Financial stock market data
- `medical_records.csv` — Patient medical records
- `news_articles.csv` — News article metadata
- `sales.csv` — E-commerce sales transactions
- `custom_space_missions.csv` — Space mission data

### Available Scripts

```bash
bun run dev           # Start Next.js dev server on port 3000
bun run build         # Production build
bun run lint          # Run ESLint
bun run db:push       # Push Prisma schema to SQLite
```

---

## 8. Environment Variables

Create a `.env` file in the project root directory:

```env
# ─── Database ────────────────────────────────────────────────
DATABASE_URL="file:./db/custom.db"

# ─── Authentication ─────────────────────────────────────────
JWT_SECRET="your-secure-jwt-secret-key-change-in-production"

# ─── LLM / AI ───────────────────────────────────────────────
OPENAI_API_KEY="sk-your-openai-api-key-here"
OPENAI_API_BASE="https://api.openai.com/v1"

# ─── LLM Configuration ──────────────────────────────────────
LLM_MODEL="gpt-4o-mini"              # Default model
LLM_TEMPERATURE_ANALYSIS="0.3"      # Analysis (lower = precise)
LLM_TEMPERATURE_SQL="0.0"           # SQL generation (0 = deterministic)
LLM_TEMPERATURE_GENERAL="0.7"       # General tasks
LLM_MAX_RETRIES="3"                 # LLM API retry count
LLM_TIMEOUT="60"                    # LLM call timeout (seconds)

# ─── Python Backend ─────────────────────────────────────────
DATAFORGE_BASE_PATH="/home/z/my-project"
BACKEND_PORT="3030"

# ─── Airbyte (Optional) ────────────────────────────────────
AIRBYTE_API_URL="http://localhost:8000"
AIRBYTE_API_TOKEN="your-airbyte-api-token"

# ─── DuckDB ─────────────────────────────────────────────────
QUERY_TIMEOUT="30"                   # Max query time (seconds)
```

### Environment Variable Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | Yes | `file:./db/custom.db` | Prisma/SQLite database connection string |
| `JWT_SECRET` | Yes | — | Secret key for signing JWT tokens (7-day expiry) |
| `OPENAI_API_KEY` | No | — | OpenAI API key — required for LLM features (analysis, NL→SQL, dbt) |
| `OPENAI_API_BASE` | No | `https://api.openai.com/v1` | OpenAI-compatible API base URL |
| `LLM_MODEL` | No | `gpt-4o-mini` | Default LLM model |
| `LLM_TEMPERATURE_ANALYSIS` | No | `0.3` | Temperature for dataset analysis |
| `LLM_TEMPERATURE_SQL` | No | `0.0` | Temperature for NL→SQL generation (deterministic) |
| `LLM_TEMPERATURE_GENERAL` | No | `0.7` | Temperature for general tasks (dbt, pipeline) |
| `LLM_MAX_RETRIES` | No | `3` | Retry count for LLM API calls |
| `LLM_TIMEOUT` | No | `60` | Timeout in seconds for LLM calls |
| `DATAFORGE_BASE_PATH` | No | `/home/z/my-project` | Base path for all data directories |
| `BACKEND_PORT` | No | `3030` | Python FastAPI server port |
| `AIRBYTE_API_URL` | No | `http://localhost:8000` | Airbyte server URL |
| `AIRBYTE_API_TOKEN` | No | — | Airbyte API authentication token |
| `QUERY_TIMEOUT` | No | `30` | DuckDB query timeout in seconds |

---

## 9. Deployment Guide

### Docker Deployment

```dockerfile
# Dockerfile (multi-stage build)
FROM node:20-alpine AS frontend-builder
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci
COPY . .
RUN npx prisma generate
RUN npm run build

FROM python:3.12-slim AS backend
WORKDIR /app
COPY mini-services/dataforge-backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY mini-services/dataforge-backend/ ./backend/

FROM node:20-alpine AS production
WORKDIR /app
COPY --from=frontend-builder /app/.next/standalone ./
COPY --from=frontend-builder /app/.next/static ./.next/static
COPY --from=frontend-builder /app/public ./public
COPY --from=backend /app/backend ./mini-services/dataforge-backend/
COPY --from=frontend-builder /app/prisma ./prisma
COPY --from=frontend-builder /app/db ./db
COPY --from=frontend-builder /app/data ./data
COPY --from=frontend-builder /app/warehouse ./warehouse
COPY --from=frontend-builder /app/pipelines ./pipelines
COPY --from=frontend-builder /app/reports ./reports
COPY --from=frontend-builder /app/dbt_project ./dbt_project

EXPOSE 3000 3030
ENV NODE_ENV=production
ENV DATABASE_URL="file:./db/custom.db"
CMD ["sh", "-c", "cd mini-services/dataforge-backend && python -m uvicorn main:app --host 0.0.0.0 --port 3030 & cd /app && node server.js"]
```

### Docker Compose

```yaml
# docker-compose.yml
version: '3.8'

services:
  frontend:
    build: .
    ports:
      - "3000:3000"
    environment:
      - DATABASE_URL=file:./db/custom.db
      - JWT_SECRET=${JWT_SECRET}
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - BACKEND_PORT=3030
    volumes:
      - ./data:/app/data
      - ./warehouse:/app/warehouse
      - ./pipelines:/app/pipelines
      - ./reports:/app/reports
      - ./dbt_project:/app/dbt_project
    depends_on:
      - backend
    restart: unless-stopped

  backend:
    build: .
    command: >
      sh -c "cd /app/mini-services/dataforge-backend &&
             python -m uvicorn main:app --host 0.0.0.0 --port 3030"
    ports:
      - "3030:3030"
    environment:
      - DATAFORGE_BASE_PATH=/app
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - QUERY_TIMEOUT=30
    volumes:
      - ./data:/app/data
      - ./warehouse:/app/warehouse
    restart: unless-stopped

  # Optional: Airbyte for external data connectors
  airbyte:
    image: airbyte/airbyte:latest
    ports:
      - "8000:8000"
    volumes:
      - airbyte_data:/var/lib/airbyte
    restart: unless-stopped

volumes:
  airbyte_data:
```

### Production Deployment Steps

```bash
# 1. Build Docker images
docker-compose build

# 2. Start all services
docker-compose up -d

# 3. Initialize database
docker-compose exec frontend npx prisma db push

# 4. Verify services
curl http://localhost:3000/api/auth    # Frontend API
curl http://localhost:3030/health      # Backend API
```

### Railway / Vercel Deployment Notes

- **Vercel**: Deploy the Next.js frontend. Set `BACKEND_PORT` env var to your Railway backend URL. Use `vercel.json` rewrites to proxy `/api/*` to the backend.
- **Railway**: Deploy the Python backend as a separate service. Set `DATAFORGE_BASE_PATH` to `/app` and expose port 3030.
- **Airbyte**: Deploy as a separate service or use Railway's Airbyte template.

### Production Checklist

| Item | Recommendation |
|---|---|
| **HTTPS / SSL** | Use Caddy or Nginx for SSL termination — never serve over plain HTTP |
| **CORS** | Configure strict CORS origins in production (replace `allow_origins=["*"]`) |
| **Rate Limiting** | Implement rate limiting on all API endpoints (use `slowapi` for FastAPI) |
| **Environment Secrets** | Use Docker secrets, Railway env vars, or a secrets manager — never commit `.env` |
| **Database Backups** | Schedule regular backups of `warehouse/warehouse.duckdb` and `db/custom.db` |
| **Monitoring** | Add health check endpoints, log aggregation, and error tracking |
| **LLM Cost Control** | Set usage limits and monitoring on LLM API calls |
| **JWT Secret** | Use a cryptographically random secret — minimum 32 characters |
| **Scaling** | Frontend: horizontal via container orchestration. Backend: scale Python workers with `gunicorn` + `uvicorn` workers |

---

## 10. Project Structure

```
dataforge-ai/
├── src/
│   ├── app/
│   │   ├── layout.tsx                    # Root layout with ThemeProvider + metadata
│   │   ├── page.tsx                      # Main app (Landing + Dashboard + Auth Dialog)
│   │   ├── globals.css                   # Global styles + Tailwind CSS 4 config
│   │   └── api/                          # 18 API route handlers
│   │       ├── route.ts                  # Root API health check
│   │       ├── auth/route.ts             # JWT login/signup/verify
│   │       ├── upload/route.ts           # File upload (multipart)
│   │       ├── files/route.ts            # List project files
│   │       ├── schema/route.ts           # Schema CRUD
│   │       ├── query/route.ts            # NL→SQL proxy
│   │       ├── agent/route.ts            # Agent command execution
│   │       ├── load-sample/route.ts      # Load sample datasets
│   │       ├── warehouse/route.ts        # Warehouse info
│   │       ├── pipelines/route.ts        # Pipeline history
│   │       ├── dashboard/
│   │       │   ├── route.ts              # Dashboard statistics
│   │       │   └── charts/route.ts       # Chart visualization data
│   │       ├── llm/
│   │       │   ├── analyze/route.ts      # LLM analysis proxy
│   │       │   └── generate-dbt/route.ts # dbt generation proxy
│   │       └── airbyte/
│   │           ├── source-definitions/route.ts
│   │           ├── sources/route.ts
│   │           ├── connections/route.ts
│   │           └── templates/[sourceType]/route.ts
│   ├── components/
│   │   ├── auth-dialog.tsx               # Login/Signup dialog (shadcn Dialog + Tabs)
│   │   ├── dataforge/                    # Domain-specific components
│   │   │   ├── command-box.tsx           # Agent command input
│   │   │   ├── file-card.tsx             # File display card
│   │   │   ├── file-explorer.tsx         # Self-fetching file browser
│   │   │   ├── navbar.tsx                # Top navigation bar
│   │   │   ├── pipeline-dag.tsx          # Pipeline execution DAG
│   │   │   ├── query-box.tsx             # NL query input + results table
│   │   │   ├── sidebar.tsx               # App sidebar navigation
│   │   │   └── stats-cards.tsx           # Dashboard stat cards
│   │   └── ui/                           # 50+ shadcn/ui components
│   ├── hooks/
│   │   ├── use-mobile.ts                 # Mobile viewport detection
│   │   └── use-toast.ts                  # Toast notification hook
│   └── lib/
│       ├── auth-store.ts                 # Zustand auth state store
│       ├── db.ts                         # Prisma client singleton
│       └── utils.ts                      # Utility functions (cn, etc.)
│
├── mini-services/
│   └── dataforge-backend/
│       ├── main.py                       # FastAPI entry point (38 endpoints)
│       ├── requirements.txt              # Python dependencies (14 packages)
│       ├── agent/
│       │   ├── master_agent.py           # Master orchestrator (LangGraph wrapper)
│       │   ├── orchestration.py          # LangGraph StateGraph (11 nodes)
│       │   ├── pipeline_generator.py     # ETL pipeline code generator
│       │   ├── query_agent.py            # NL→SQL agent (pattern + LLM)
│       │   ├── utils.py                  # Shared utilities (paths, SQL safety)
│       │   ├── schemas.py                # Pydantic output schemas (10 models)
│       │   └── tools/
│       │       ├── __init__.py
│       │       ├── llm_agent.py          # LLM analysis (LangChain ChatOpenAI)
│       │       ├── schema_detector.py    # Auto schema detection
│       │       ├── duckdb_tool.py        # DuckDB warehouse operations
│       │       ├── universal_duckdb_tool.py
│       │       ├── universal_query_agent.py
│       │       ├── dbt_tool.py           # dbt model management
│       │       ├── report_tool.py        # Report generation
│       │       ├── airbyte_tool.py       # Airbyte integration
│       │       ├── airbyte_connector.py  # Airbyte HTTP client
│       │       └── xlsx_processor.py     # Excel file processor
│       └── utils/
│           ├── __init__.py
│           └── config.py                 # Backend configuration
│
├── data/
│   ├── raw/                              # Raw uploaded files (CSV, JSON, XLSX)
│   ├── clean/                            # Cleaned/transformed data
│   └── samples/                          # Built-in sample datasets
│
├── warehouse/
│   ├── warehouse.duckdb                  # DuckDB data warehouse
│   └── schema_cache.json                 # Auto-detected schema cache
│
├── pipelines/                            # Generated pipeline files
├── reports/                              # Generated reports (CSV, JSON)
├── dbt_project/                          # dbt project (models, tests)
├── db/                                   # Prisma SQLite database
├── prisma/
│   └── schema.prisma                     # Prisma schema (User, Post)
├── public/                               # Static assets
├── package.json                          # Node.js dependencies
├── tailwind.config.ts                    # Tailwind CSS configuration
├── tsconfig.json                         # TypeScript configuration
├── next.config.ts                        # Next.js configuration
└── Caddyfile                             # Caddy reverse proxy config
```

---

<div align="center">

**Omnix AI** — LLM-Powered Data Pipeline Platform

Built with Next.js 16 · FastAPI · LangGraph · DuckDB · LangChain

</div>
