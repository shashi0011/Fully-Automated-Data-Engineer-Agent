"""
DataForge AI - LLM Agent (LangChain Rewrite)
Uses LangChain ChatOpenAI with structured output for all LLM interactions.
Provides dataset analysis, NL→SQL, dbt model generation, and pipeline generation.

FALLBACK SUPPORT: If the primary LLM fails (timeout, rate limit, API error),
automatically retries with a secondary fallback model. Max 2 attempts total.
"""

import os
import json
import re
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from langchain_openai import ChatOpenAI

from agent.schemas import (
    ColumnAnalysis,
    DataQualitySummary,
    DatasetAnalysis,
    SQLResult,
    DBTModel,
    DBTModelsOutput,
    RecommendedTransformation,
    SuggestedMetric,
)
from agent.utils import (
    categorize_columns,
    validate_sql,
    DBT_DIR,
)

logger = logging.getLogger(__name__)


class LLMAgent:
    """LLM-powered agent for intelligent data engineering operations.

    Uses LangChain ChatOpenAI with structured output (Pydantic models) for
    reliable, typed responses from the LLM. Falls back to heuristic-based
    generation when the LLM is unavailable.
    """

    _MAX_SESSION_MESSAGES = 20
    _sessions: Dict[str, List[Dict[str, str]]] = {}

    def __init__(self) -> None:
        # ── Primary LLM config ──
        self.api_base: str = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
        self.api_key: str = os.getenv("OPENAI_API_KEY", "")
        self.model: str = os.getenv("LLM_MODEL", "gpt-4o-mini")

        # ── Fallback LLM config ──
        self.fallback_api_key: str = os.getenv("FALLBACK_API_KEY", "")
        self.fallback_api_base: str = os.getenv("FALLBACK_API_BASE", "")
        self.fallback_model: str = os.getenv("FALLBACK_MODEL", "")
        self._fallback_configured: bool = bool(self.fallback_model)

        self._available: bool = bool(self.api_key)

        if self._available:
            common_kwargs = {
                "model": self.model,
                "api_key": self.api_key,
                "base_url": self.api_base,
                "max_retries": 3,
                "timeout": 60,
            }
            self.llm_sql: Optional[ChatOpenAI] = ChatOpenAI(temperature=0.0, **common_kwargs)
            self.llm_analysis: Optional[ChatOpenAI] = ChatOpenAI(temperature=0.3, **common_kwargs)
            self.llm_general: Optional[ChatOpenAI] = ChatOpenAI(temperature=0.7, **common_kwargs)
        else:
            self.llm_sql = None
            self.llm_analysis = None
            self.llm_general = None

        # ── Create fallback LLM instances if configured ──
        self.llm_sql_fallback: Optional[ChatOpenAI] = None
        self.llm_analysis_fallback: Optional[ChatOpenAI] = None
        self.llm_general_fallback: Optional[ChatOpenAI] = None

        if self._fallback_configured:
            fb_kwargs = {
                "model": self.fallback_model,
                "api_key": self.fallback_api_key or self.api_key,
                "base_url": self.fallback_api_base or self.api_base,
                "max_retries": 2,
                "timeout": 60,
            }
            self.llm_sql_fallback = ChatOpenAI(temperature=0.0, **fb_kwargs)
            self.llm_analysis_fallback = ChatOpenAI(temperature=0.3, **fb_kwargs)
            self.llm_general_fallback = ChatOpenAI(temperature=0.7, **fb_kwargs)
            logger.info(
                f"Fallback LLM configured: {self.fallback_model} @ "
                f"{self.fallback_api_base or self.api_base}"
            )

    # ── Session memory helpers ────────────────────────────────────────────────

    def _session_messages(self, session_id: str) -> List[Dict[str, str]]:
        if session_id not in self._sessions:
            self._sessions[session_id] = []
        return self._sessions[session_id]

    def _push_session(self, session_id: str, role: str, content: str) -> None:
        msgs = self._session_messages(session_id)
        msgs.append({"role": role, "content": content})
        if len(msgs) > self._MAX_SESSION_MESSAGES:
            del msgs[: len(msgs) - self._MAX_SESSION_MESSAGES]

    # ── Fallback helper ─────────────────────────────────────────────────────

    async def _invoke_llm_with_fallback(
        self,
        primary_llm: Optional[ChatOpenAI],
        fallback_llm: Optional[ChatOpenAI],
        messages: List[Dict[str, str]],
        structured_output_type: Optional[Any] = None,
    ) -> Any:
        """Try primary LLM, fall back to secondary on failure."""
        if primary_llm is None and fallback_llm is None:
            raise RuntimeError("No LLM available — primary and fallback both unconfigured")

        # ── Attempt 1: Primary ──
        if primary_llm is not None:
            try:
                if structured_output_type:
                    chain = primary_llm.with_structured_output(structured_output_type)
                    result = await chain.ainvoke(messages)
                else:
                    response = await primary_llm.ainvoke(messages)
                    result = response.content
                logger.info(f"[Primary] {primary_llm.model} succeeded")
                return result
            except Exception as e:
                logger.warning(f"[Primary] {primary_llm.model} failed: {type(e).__name__}: {e}")
        else:
            logger.warning("[Primary] No primary LLM configured")

        # ── Attempt 2: Fallback ──
        if fallback_llm is not None:
            try:
                logger.info(f"[Fallback] Trying {fallback_llm.model}...")
                if structured_output_type:
                    chain = fallback_llm.with_structured_output(structured_output_type)
                    result = await chain.ainvoke(messages)
                else:
                    response = await fallback_llm.ainvoke(messages)
                    result = response.content
                logger.info(f"[Fallback] {fallback_llm.model} succeeded")
                return result
            except Exception as e:
                logger.error(f"[Fallback] {fallback_llm.model} also failed: {type(e).__name__}: {e}")
        else:
            logger.warning("[Fallback] No fallback LLM configured")

        raise RuntimeError(
            f"Both primary and fallback LLMs failed. "
            f"Primary: {self.model}, Fallback: {self.fallback_model or 'not configured'}"
        )

    # ── Health check ──────────────────────────────────────────────────────────

    def check_health(self) -> Dict[str, Any]:
        return {
            "available": self._available,
            "model": self.model if self._available else None,
            "api_base": self.api_base if self._available else None,
            "api_key_set": bool(self.api_key),
            "sessions_active": len(self._sessions),
            "fallback_configured": self._fallback_configured,
            "fallback_model": self.fallback_model if self._fallback_configured else None,
            "fallback_api_base": self.fallback_api_base if self._fallback_configured else None,
        }

    # ── Dataset Analysis ──────────────────────────────────────────────────────

    async def analyze_dataset(
        self,
        schema_info: Dict[str, Any],
        sample_data: List[Dict],
        session_id: str = "default",
    ) -> Dict[str, Any]:
        if not self._available or self.llm_analysis is None:
            return self._generate_fallback_analysis(schema_info)

        table_name = schema_info.get("table_name", "data_clean")
        columns = schema_info.get("columns", {})
        dataset_type_hint = schema_info.get("dataset_type", "generic")

        col_desc_parts: List[str] = []
        for col_name, col_info in columns.items():
            dtype = col_info.get("type", "unknown")
            semantic = col_info.get("semantic", "unknown")
            sample_val = ""
            if sample_data and col_name in sample_data[0]:
                sample_val = str(sample_data[0][col_name])
            col_desc_parts.append(
                f"  - {col_name} (type={dtype}, semantic={semantic}, sample={sample_val})"
            )

        col_desc = "\n".join(col_desc_parts) if col_desc_parts else "  (no columns detected)"
        sample_str = json.dumps(sample_data[:5], indent=2, default=str) if sample_data else "[]"

        system_prompt = (
            "You are an expert data engineer and analyst. Analyze the provided "
            "dataset schema and sample data to:\n"
            "1. Identify the exact dataset type\n"
            "2. Understand the semantic meaning of each column\n"
            "3. Identify data quality issues\n"
            "4. Recommend appropriate transformations\n"
            "5. Suggest meaningful metrics and KPIs\n\n"
            f"dataset_type='{dataset_type_hint}', table_name='{table_name}'.\n"
            "Respond ONLY with valid JSON matching the requested schema."
        )

        user_prompt = (
            f"Analyze this dataset:\n\nTable: {table_name}\nColumns:\n{col_desc}\n\n"
            f"Sample Data (first 5 rows):\n{sample_str}\n\nProvide a comprehensive analysis."
        )

        self._push_session(session_id, "user", user_prompt)
        messages = [
            {"role": "system", "content": system_prompt},
            *self._session_messages(session_id),
        ]

        try:
            result: DatasetAnalysis = await self._invoke_llm_with_fallback(
                self.llm_analysis,
                self.llm_analysis_fallback,
                messages,
                structured_output_type=DatasetAnalysis,
            )
            self._push_session(session_id, "assistant", result.model_dump_json())
            return {"llm_used": True, **result.model_dump()}
        except Exception as exc:
            logger.error(f"[LLMAgent] analyze_dataset LLM error (primary + fallback): {exc}")
            return self._generate_fallback_analysis(schema_info)

    # ── NL → SQL ──────────────────────────────────────────────────────────────

    async def generate_sql(
        self,
        question: str,
        schema_info: Dict[str, Any],
        session_id: str = "default",
    ) -> Dict[str, Any]:
        if not self._available or self.llm_sql is None:
            return self._generate_fallback_sql(question, schema_info)

        table_name = schema_info.get("table_name", "data_clean")
        columns = schema_info.get("columns", {})
        dataset_type = schema_info.get("dataset_type", "generic")

        col_lines = "\n".join(
            f"  - {c} ({info.get('type', '?')}, semantic={info.get('semantic', '?')})"
            for c, info in columns.items()
        )

        system_prompt = (
            "You are an expert DuckDB SQL developer. Convert natural language questions "
            "to accurate DuckDB SQL queries.\n\n"
            f"Table: {table_name}\nDataset Type: {dataset_type}\n\n"
            f"Available Columns:\n{col_lines}\n\n"
            "Rules:\n1. Generate ONLY valid DuckDB SQL\n"
            "2. Use proper aggregation functions\n"
            "3. Include ORDER BY when asking for top/bottom\n"
            "4. Use LIMIT appropriately (default 20 rows)\n"
            "5. Handle date formatting with strftime\n"
            "6. Do NOT use dangerous SQL (DROP, DELETE, TRUNCATE)\n"
            "7. Wrap column names in double quotes if they contain special characters"
        )

        user_prompt = f'Convert this question to SQL:\n"{question}"\n\nProvide the SQL and explain what it does.'
        self._push_session(session_id, "user", user_prompt)

        messages = [
            {"role": "system", "content": system_prompt},
            *self._session_messages(session_id),
        ]

        try:
            result: SQLResult = await self._invoke_llm_with_fallback(
                self.llm_sql,
                self.llm_sql_fallback,
                messages,
                structured_output_type=SQLResult,
            )
            self._push_session(session_id, "assistant", result.model_dump_json())

            sql = result.sql
            is_safe, reason = validate_sql(sql)
            if not is_safe:
                logger.warning(f"[LLMAgent] SQL validation blocked: {reason}")
                return self._generate_fallback_sql(question, schema_info)

            return {"llm_used": True, **result.model_dump()}
        except Exception as exc:
            logger.error(f"[LLMAgent] generate_sql LLM error (primary + fallback): {exc}")
            return self._generate_fallback_sql(question, schema_info)

    async def generate_sql_from_question(
        self,
        question: str,
        schema_info: Dict[str, Any],
        analysis: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Alias for generate_sql — backward compatibility."""
        return await self.generate_sql(question, schema_info)

    # ── dbt Model Generation ──────────────────────────────────────────────────

    async def generate_dbt_models(
        self,
        schema_info: Dict[str, Any],
        analysis: Optional[Dict[str, Any]] = None,
        session_id: str = "default",
    ) -> Dict[str, Any]:
        if not self._available or self.llm_general is None:
            return self._generate_fallback_dbt(schema_info, analysis or {})

        table_name = schema_info.get("table_name", "data_clean")
        raw_table = schema_info.get("raw_table", "data_raw")
        columns = schema_info.get("columns", {})

        dataset_type = "generic"
        transformations = []
        if analysis:
            dataset_type = analysis.get("dataset_type", "generic")
            transformations = analysis.get("recommended_transformations", [])

        col_lines = "\n".join(
            f"  - {c} ({info.get('type', '?')}, semantic={info.get('semantic', '?')})"
            for c, info in columns.items()
        )
        transform_str = json.dumps(transformations, indent=2, default=str) if transformations else "[]"

        system_prompt = (
            "You are an expert dbt developer. Generate production-quality dbt models with:\n"
            "1. Clear, readable SQL with CTEs\n"
            "2. Proper comments explaining business logic\n"
            "3. Data quality tests described in comments\n"
            "4. Models for: staging, intermediate, marts\n\n"
            "Include schema.yml content as a model with path ending in schema.yml.\n"
            "All SQL must be DuckDB-compatible."
        )

        user_prompt = (
            f"Generate dbt models for this {dataset_type} dataset:\n\n"
            f"Table Name: {table_name}\nSource Table: {raw_table}\n\n"
            f"Columns:\n{col_lines}\n\n"
            f"Recommended Transformations:\n{transform_str}\n\n"
            "Generate comprehensive dbt models."
        )

        self._push_session(session_id, "user", user_prompt)
        messages = [
            {"role": "system", "content": system_prompt},
            *self._session_messages(session_id),
        ]

        try:
            result: DBTModelsOutput = await self._invoke_llm_with_fallback(
                self.llm_general,
                self.llm_general_fallback,
                messages,
                structured_output_type=DBTModelsOutput,
            )
            self._push_session(session_id, "assistant", result.model_dump_json())

            models_list = [m.model_dump() for m in result.models]
            schema_yml = ""
            tests = []
            documentation = ""

            for m in models_list:
                if m.get("path", "").endswith("schema.yml"):
                    schema_yml = m["content"]
                    documentation = m.get("description", "")
                else:
                    tests.append(f"Model: {m.get('path', '?')}")

            return {
                "llm_used": True,
                "models": models_list,
                "schema_yml": schema_yml,
                "tests": tests,
                "documentation": documentation,
            }
        except Exception as exc:
            logger.error(f"[LLMAgent] generate_dbt_models LLM error (primary + fallback): {exc}")
            return self._generate_fallback_dbt(schema_info, analysis or {})

    # ── Pipeline Code Generation ──────────────────────────────────────────────

    async def generate_pipeline_code(
        self,
        schema_info: Dict[str, Any],
        analysis: Dict[str, Any],
        operations: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        if not self._available or self.llm_general is None:
            return self._generate_fallback_pipeline(schema_info, analysis, operations)

        table_name = schema_info.get("table_name", "data_clean")
        raw_table = schema_info.get("raw_table", "data_raw")
        dataset_type = analysis.get("dataset_type", schema_info.get("dataset_type", "generic"))
        columns = schema_info.get("columns", {})

        if operations is None:
            operations = ["extract", "clean", "transform", "analyze", "report"]

        col_lines = "\n".join(
            f"  - {c} ({info.get('type', '?')}, semantic={info.get('semantic', '?')})"
            for c, info in columns.items()
        )
        transform_str = json.dumps(
            analysis.get("recommended_transformations", []), indent=2, default=str
        )

        system_prompt = (
            "You are an expert data engineer. Generate production-quality Prefect pipeline "
            "code for data processing.\n\n"
            "The pipeline should:\n"
            "1. Have clear, descriptive task functions\n"
            "2. Include proper error handling and retries\n"
            "3. Have detailed docstrings\n"
            "4. Follow Python best practices\n"
            "5. Include type hints\n\n"
            'Respond in JSON format:\n'
            '{"pipeline_code": "complete Python code", "config": {"schedule": "...", "retries": 3}, '
            '"description": "what this pipeline does", "tasks": ["task1", "task2"]}'
        )

        user_prompt = (
            f"Generate a Prefect pipeline for this {dataset_type} dataset:\n\n"
            f"Table: {table_name}\nSource Table: {raw_table}\n"
            f"Operations: {operations}\n\nSchema:\n{col_lines}\n\n"
            f"Recommended Transformations:\n{transform_str}\n\n"
            "Generate complete, production-ready pipeline code."
        )

        try:
            content = await self._invoke_llm_with_fallback(
                self.llm_general,
                self.llm_general_fallback,
                [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )

            if content:
                if "```json" in content:
                    content = content.split("```json")[1].split("```")[0]
                elif "```" in content:
                    content = content.split("```")[1].split("```")[0]
                parsed = json.loads(content.strip())
                parsed["llm_used"] = True
                return parsed
        except (json.JSONDecodeError, KeyError, IndexError) as exc:
            logger.error(f"[LLMAgent] generate_pipeline_code parse error: {exc}")
        except Exception as exc:
            logger.error(f"[LLMAgent] generate_pipeline_code LLM error (primary + fallback): {exc}")

        return self._generate_fallback_pipeline(schema_info, analysis, operations)

    # ── Fallback: Pattern-Based SQL ───────────────────────────────────────────

    def _generate_fallback_sql(self, question: str, schema_info: Dict[str, Any]) -> Dict[str, Any]:
        """Generate SQL based on keyword patterns when LLM is unavailable."""
        table_name = schema_info.get("table_name", "data_clean")
        columns = schema_info.get("columns", {})
        question_lower = question.lower()

        cats = categorize_columns(columns)
        numeric_cols = cats["numeric"]
        category_cols = cats["categorical"]
        date_cols = cats["date"]

        sql = f"SELECT * FROM {table_name} LIMIT 20"
        explanation = "Show all records"

        if any(kw in question_lower for kw in ("top", "highest", "best")):
            if numeric_cols and category_cols:
                limit_match = re.search(r"top\s+(\d+)", question_lower)
                limit = int(limit_match.group(1)) if limit_match else 10
                sql = (
                    f"SELECT {category_cols[0]}, SUM({numeric_cols[0]}) as total "
                    f"FROM {table_name} GROUP BY {category_cols[0]} "
                    f"ORDER BY total DESC LIMIT {limit}"
                )
                explanation = f"Top {limit} {category_cols[0]} by total {numeric_cols[0]}"

        elif any(kw in question_lower for kw in ("average", "avg")):
            if numeric_cols:
                sql = f"SELECT AVG({numeric_cols[0]}) as avg_{numeric_cols[0]} FROM {table_name}"
                explanation = f"Average {numeric_cols[0]}"

        elif any(kw in question_lower for kw in ("total", "sum")):
            if numeric_cols:
                sql = (
                    f"SELECT SUM({numeric_cols[0]}) as total_{numeric_cols[0]}, "
                    f"COUNT(*) as record_count FROM {table_name}"
                )
                explanation = f"Total {numeric_cols[0]} and record count"

        elif "by" in question_lower and category_cols and numeric_cols:
            sql = (
                f"SELECT {category_cols[0]}, SUM({numeric_cols[0]}) as total, "
                f"COUNT(*) as count FROM {table_name} "
                f"GROUP BY {category_cols[0]} ORDER BY total DESC"
            )
            explanation = f"Group by {category_cols[0]} with totals"

        elif any(kw in question_lower for kw in ("trend", "over time", "monthly")):
            if date_cols and numeric_cols:
                sql = (
                    f"SELECT strftime({date_cols[0]}, '%Y-%m') as period, "
                    f"SUM({numeric_cols[0]}) as total FROM {table_name} "
                    f"GROUP BY period ORDER BY period"
                )
                explanation = f"Monthly trend of {numeric_cols[0]}"

        elif any(kw in question_lower for kw in ("count", "how many")):
            if category_cols:
                sql = (
                    f"SELECT {category_cols[0]}, COUNT(*) as count FROM {table_name} "
                    f"GROUP BY {category_cols[0]} ORDER BY count DESC"
                )
                explanation = f"Count by {category_cols[0]}"
            else:
                sql = f"SELECT COUNT(*) as total_count FROM {table_name}"
                explanation = "Total count of records"

        return {
            "sql": sql,
            "explanation": explanation,
            "generated_by": "fallback",
            "llm_used": False,
            "error": None,
        }

    # ── Fallback: Heuristic Analysis ──────────────────────────────────────────

    def _generate_fallback_analysis(self, schema_info: Dict[str, Any]) -> Dict[str, Any]:
        columns = schema_info.get("columns", {})
        dataset_type = schema_info.get("dataset_type", "generic")

        cats = categorize_columns(columns)
        numeric_cols = cats["numeric"]
        category_cols = cats["categorical"]
        date_cols = cats["date"]
        text_cols = cats["text"]
        id_cols = cats["id"]

        column_analysis: Dict[str, Dict[str, Any]] = {}
        for col_name, col_info in columns.items():
            semantic = col_info.get("semantic", "generic")
            dtype = col_info.get("type", "")
            issues: List[str] = []
            recommendations: List[str] = []

            nullable = col_info.get("nullable", True)
            if nullable:
                issues.append("Column allows NULL values")
                recommendations.append("Consider whether NULLs need imputation")

            if semantic in ("money", "count", "score", "percentage"):
                recommendations.append("Consider aggregation metrics")
            elif semantic in ("category", "location", "name"):
                recommendations.append("Good candidate for grouping and filtering")
            elif semantic == "datetime":
                recommendations.append("Consider time-based trend analysis")
            elif semantic == "id":
                recommendations.append("Use as join key, not for aggregation")

            column_analysis[col_name] = {
                "semantic_type": semantic,
                "business_meaning": f"Contains {semantic} data ({dtype})",
                "data_quality": "good" if not nullable else "moderate",
                "issues": issues,
                "recommendations": recommendations,
            }

        transformations: List[Dict[str, str]] = []
        if numeric_cols and category_cols:
            transformations.append({
                "type": "aggregate",
                "description": f"Aggregate {numeric_cols[0]} by {category_cols[0]}",
                "sql_template": f"SELECT {category_cols[0]}, SUM({numeric_cols[0]}) as total FROM table GROUP BY {category_cols[0]}",
            })
        if date_cols and numeric_cols:
            transformations.append({
                "type": "derive",
                "description": f"Time-based analysis of {numeric_cols[0]}",
                "sql_template": f"SELECT strftime({date_cols[0]}, '%Y-%m') as period, SUM({numeric_cols[0]}) as total FROM table GROUP BY period",
            })

        suggested_metrics: List[Dict[str, str]] = [
            {"name": "Total Records", "description": "Count of all records", "formula": "COUNT(*)", "business_value": "Understand data volume"}
        ]
        if numeric_cols:
            suggested_metrics.append({
                "name": f"Total {numeric_cols[0]}", "description": f"Sum of all {numeric_cols[0]} values",
                "formula": f"SUM({numeric_cols[0]})", "business_value": "Understand total magnitude"
            })

        insights = [f"This appears to be a {dataset_type} dataset with {len(columns)} columns"]
        if numeric_cols:
            insights.append(f"Found {len(numeric_cols)} numeric column(s): {', '.join(numeric_cols)}")
        if category_cols:
            insights.append(f"Found {len(category_cols)} categorical column(s): {', '.join(category_cols)}")
        if date_cols:
            insights.append(f"Found {len(date_cols)} date column(s): {', '.join(date_cols)}")

        return {
            "llm_used": False,
            "error": "LLM not available — using heuristic analysis",
            "dataset_type": dataset_type,
            "dataset_subtype": "standard",
            "confidence_score": 0.0,
            "column_analysis": column_analysis,
            "data_quality_summary": {
                "overall_score": 0.0,
                "issues": ["LLM unavailable — data quality not assessed"],
                "recommendations": ["Connect an LLM API key for full analysis"],
            },
            "recommended_transformations": transformations,
            "suggested_metrics": suggested_metrics,
            "natural_language_insights": insights,
        }

    # ── Fallback: Template dbt Models ─────────────────────────────────────────

    def _generate_fallback_dbt(self, schema_info: Dict[str, Any], analysis: Dict[str, Any]) -> Dict[str, Any]:
        schema_info = schema_info or {}
        table_name = schema_info.get("table_name", "data_clean")
        raw_table = schema_info.get("raw_table", "data_raw")
        columns = schema_info.get("columns", {})
        dataset_type = analysis.get("dataset_type", "generic")

        cats = categorize_columns(columns)
        numeric_cols = cats["numeric"]
        category_cols = cats["categorical"]
        date_cols = cats["date"]
        id_cols = cats["id"]

        # ── Staging model ──
        id_lines = "\n".join(f"        {c} as {c}," for c in id_cols[:3]) if id_cols else "        -- No ID columns detected"
        cat_lines = "\n".join(f"        trim({c}) as {c}," for c in category_cols[:3]) if category_cols else "        -- No category columns"
        num_lines = "\n".join(f"        round({c}::numeric, 2) as {c}," for c in numeric_cols[:3]) if numeric_cols else "        -- No numeric columns"
        date_lines = "\n".join(f"        cast({c} as date) as {c}," for c in date_cols[:2]) if date_cols else "        -- No date columns"
        where_clause = f"        and {id_cols[0]} is not null" if id_cols else ""

        staging = (
            f"-- DataForge AI Generated dbt Model (fallback — no LLM)\n"
            f"-- Staging: {table_name}\n"
            f"-- Dataset Type: {dataset_type}\n"
            f"-- Generated at: {datetime.now().isoformat()}\n\n"
            f"with source as (\n"
            f"    select * from {{{{ source('raw', '{raw_table}') }}}}\n"
            f"),\n\n"
            f"cleaned as (\n"
            f"    select\n"
            f"        -- Primary identifiers\n"
            f"{id_lines}\n\n"
            f"        -- Dimensional columns\n"
            f"{cat_lines}\n\n"
            f"        -- Metrics\n"
            f"{num_lines}\n\n"
            f"        -- Time dimensions\n"
            f"{date_lines}\n\n"
            f"        -- Metadata\n"
            f"        current_timestamp as _loaded_at\n\n"
            f"    from source\n"
            f"    where 1=1\n"
            f"{where_clause}\n"
            f")\n\n"
            f"select * from cleaned\n"
        )

        # ── Intermediate model ──
        agg_col = category_cols[0] if category_cols else "category"
        metric_col = numeric_cols[0] if numeric_cols else "value"

        intermediate = (
            f"-- DataForge AI Generated dbt Model (fallback — no LLM)\n"
            f"-- Intermediate: Aggregated analysis for {dataset_type} data\n\n"
            f"with base as (\n"
            f"    select * from {{{{ ref('stg_{table_name}') }}}}\n"
            f"),\n\n"
            f"aggregated as (\n"
            f"    select\n"
            f"        {agg_col},\n"
            f"        count(*) as record_count,\n"
            f"        sum({metric_col}) as total_{metric_col},\n"
            f"        avg({metric_col}) as avg_{metric_col},\n"
            f"        min({metric_col}) as min_{metric_col},\n"
            f"        max({metric_col}) as max_{metric_col}\n"
            f"    from base\n"
            f"    group by {agg_col}\n"
            f")\n\n"
            f"select * from aggregated\n"
            f"order by total_{metric_col} desc\n"
        )

        # ── Mart model ──
        mart = (
            f"-- DataForge AI Generated dbt Model (fallback — no LLM)\n"
            f"-- Mart: Final analytics table for {dataset_type} data\n\n"
            f"with staging as (\n"
            f"    select * from {{{{ ref('stg_{table_name}') }}}}\n"
            f"),\n\n"
            f"aggregated as (\n"
            f"    select * from {{{{ ref('int_{dataset_type}_aggregated') }}}}\n"
            f"),\n\n"
            f"final as (\n"
            f"    select\n"
            f"        s.*,\n"
            f"        a.record_count,\n"
            f"        a.total_{metric_col},\n"
            f"        a.avg_{metric_col}\n"
            f"    from staging s\n"
            f"    left join aggregated a on s.{agg_col} = a.{agg_col}\n"
            f")\n\n"
            f"select * from final\n"
        )

        # ── schema.yml ──
        pk_col = id_cols[0] if id_cols else "id"
        schema_yml = (
            f"version: 2\n\n"
            f"sources:\n"
            f"  - name: raw\n"
            f"    database: main\n"
            f"    schema: main\n"
            f"    tables:\n"
            f"      - name: {raw_table}\n"
            f"        description: \"Raw {dataset_type} data ingested from source\"\n\n"
            f"models:\n"
            f"  - name: stg_{table_name}\n"
            f"    description: \"Staging model for {dataset_type} data — cleaned and standardized\"\n"
            f"    columns:\n"
            f"      - name: {pk_col}\n"
            f"        description: \"Primary key\"\n"
            f"        tests:\n"
            f"          - unique\n"
            f"          - not_null\n"
            f"      - name: {agg_col}\n"
            f"        description: \"{agg_col} dimension\"\n"
            f"        tests:\n"
            f"          - not_null\n\n"
            f"  - name: int_{dataset_type}_aggregated\n"
            f"    description: \"Intermediate model with aggregated metrics by {agg_col}\"\n"
            f"    columns:\n"
            f"      - name: {agg_col}\n"
            f"        tests:\n"
            f"          - unique\n"
            f"          - not_null\n\n"
            f"  - name: mart_{dataset_type}_analytics\n"
            f"    description: \"Final analytics mart combining all transformations\"\n"
        )

        models = [
            {
                "path": f"models/staging/stg_{table_name}.sql",
                "content": staging,
                "description": f"Staging model for {dataset_type} data",
            },
            {
                "path": f"models/intermediate/int_{dataset_type}_aggregated.sql",
                "content": intermediate,
                "description": f"Aggregated analysis for {dataset_type}",
            },
            {
                "path": f"models/marts/mart_{dataset_type}_analytics.sql",
                "content": mart,
                "description": f"Final analytics mart for {dataset_type}",
            },
            {
                "path": "models/schema.yml",
                "content": schema_yml,
                "description": f"dbt schema definitions for {dataset_type} project",
            },
        ]

        return {
            "llm_used": False,
            "error": "LLM not available — using template-based dbt generation",
            "models": models,
            "schema_yml": schema_yml,
            "tests": [f"Unique and not_null test on {pk_col}", f"Not_null test on {agg_col}"],
            "documentation": f"Template-based dbt project for {dataset_type} data (staging, intermediate, marts).",
        }

    # ── Fallback: Template Pipeline ───────────────────────────────────────────

    def _generate_fallback_pipeline(
        self,
        schema_info: Dict[str, Any],
        analysis: Dict[str, Any],
        operations: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Generate pipeline code from template when LLM is unavailable."""
        table_name = schema_info.get("table_name", "data_clean")
        raw_table = schema_info.get("raw_table", "data_raw")
        dataset_type = analysis.get("dataset_type", "generic")

        if operations is None:
            operations = ["extract", "clean", "transform", "analyze", "report"]

        pipeline_code = f'''"""
DataForge AI - Auto-Generated Pipeline (fallback — no LLM)
Dataset: {dataset_type}
Generated: {datetime.now().isoformat()}
"""

from prefect import flow, task
from datetime import datetime
import duckdb
import pandas as pd
from typing import Dict, Any

# Configuration
BASE_PATH = "/home/z/my-project"
WAREHOUSE_PATH = f"{{BASE_PATH}}/warehouse/warehouse.duckdb"
RAW_TABLE = "{raw_table}"
CLEAN_TABLE = "{table_name}"


@task(retries=3, retry_delay_seconds=60)
def extract_data(source_path: str = None) -> pd.DataFrame:
    """Extract data from source file or warehouse."""
    import os
    if source_path and os.path.exists(source_path):
        if source_path.endswith('.csv'):
            df = pd.read_csv(source_path)
        elif source_path.endswith('.json'):
            df = pd.read_json(source_path)
        else:
            raise ValueError(f"Unsupported file format: {{source_path}}")
        print(f"Extracted {{len(df)}} rows from {{source_path}}")
        return df
    con = duckdb.connect(WAREHOUSE_PATH)
    df = con.execute(f"SELECT * FROM {{RAW_TABLE}}").fetchdf()
    con.close()
    print(f"Extracted {{len(df)}} rows from warehouse")
    return df


@task
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate data."""
    initial_count = len(df)
    df = df.drop_duplicates()
    df = df.dropna(how='all')
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
        df.loc[df[col] == 'nan', col] = None
    print(f"Cleaned data: {{initial_count}} -> {{len(df)}} rows")
    return df


@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Apply business transformations."""
    df['_processed_at'] = datetime.now()
    df['_record_id'] = range(1, len(df) + 1)
    return df


@task
def analyze_data(df: pd.DataFrame) -> Dict[str, Any]:
    """Generate analysis and insights."""
    return {{
        "total_records": len(df),
        "columns": list(df.columns),
        "processing_date": datetime.now().isoformat(),
    }}


@task
def load_to_warehouse(df: pd.DataFrame, table_name: str = CLEAN_TABLE) -> str:
    """Load data to DuckDB warehouse."""
    con = duckdb.connect(WAREHOUSE_PATH)
    con.execute(f"CREATE OR REPLACE TABLE {{table_name}} AS SELECT * FROM df")
    count = con.execute(f"SELECT COUNT(*) FROM {{table_name}}").fetchone()[0]
    con.close()
    return f"Loaded {{count}} rows into {{table_name}}"


@flow(name="{dataset_type}_pipeline")
def {dataset_type}_pipeline(source_path: str = None):
    """Complete data pipeline for {dataset_type} data."""
    raw_data = extract_data(source_path)
    clean_df = clean_data(raw_data)
    transformed_df = transform_data(clean_df)
    analysis = analyze_data(transformed_df)
    load_status = load_to_warehouse(transformed_df)
    return {{"status": "success", "records_processed": analysis["total_records"]}}


if __name__ == "__main__":
    {dataset_type}_pipeline()
'''

        return {
            "llm_used": False,
            "error": "LLM not available — using template-based pipeline generation",
            "pipeline_code": pipeline_code,
            "config": {"schedule": "0 6 * * *", "retries": 3, "timeout": 3600},
            "description": f"Template pipeline for {dataset_type} data with extraction, cleaning, transformation, and reporting",
            "tasks": operations,
        }
