"""
DataForge AI - Schema Detector
Auto-detects schema from any CSV/JSON file and generates rich metadata.
Uses heuristic classification by default — NO LLM dependency.
LLM is optional enhancement only.
"""

import os
import re
import json
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
logger = logging.getLogger(__name__)

from agent.utils import (
    SCHEMA_CACHE_PATH,
    save_schema,
    load_schema,
    quote_identifier,
    validate_identifier,
    categorize_columns,
)


class SchemaDetector:
    """Auto-detects schema from data files — works entirely without LLM."""

    def __init__(self):
        pass

    @staticmethod
    def _read_csv_robust(file_path: str, nrows: Optional[int] = None) -> pd.DataFrame:
        """Read CSV with fallback when row widths are inconsistent."""
        try:
            return pd.read_csv(file_path, nrows=nrows)
        except (pd.errors.ParserError, UnicodeDecodeError, ValueError):
            return pd.read_csv(
                file_path,
                nrows=nrows,
                engine="python",
                on_bad_lines="skip",
            )

    # ──────────────────────────────────────────────────────────────────
    # Column type detection (pure heuristic, no network calls)
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _convert_to_native(value):
        """Convert numpy types to Python native types for JSON serialization"""
        import numpy as np
        import pandas as pd
        if pd.isna(value):
            return None
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating,)):
            return float(value)
        if isinstance(value, (np.bool_,)):
            return bool(value)
        if isinstance(value, (np.ndarray,)):
            return value.tolist()
        if isinstance(value, pd.Timestamp):
            return str(value)
        return value

    @staticmethod
    def _detect_column_type(series: pd.Series) -> Dict[str, Any]:
        """Detect the semantic type, dtype, and stats of a column."""
        col_name = str(series.name).lower()
        non_null = series.dropna()

        if len(non_null) == 0:
            return {"type": "unknown", "semantic": "unknown", "nullable": True}

        dtype = str(series.dtype)
        sample_values = [
            SchemaDetector._convert_to_native(v) for v in non_null.head(5).tolist()
        ]
        unique_count = int(len(non_null.unique()))
        total_count = int(len(non_null))
        null_count = int(series.isnull().sum())
        nullable = null_count > 0

        info: Dict[str, Any] = {
            "type": dtype,
            "semantic": "generic",
            "nullable": nullable,
            "sample_values": sample_values[:3],
            "unique_count": unique_count,
            "total_count": total_count,
            "null_count": null_count,
            "null_pct": round(null_count / len(series) * 100, 1) if len(series) > 0 else 0,
        }

        # ── Semantic classification by column name ──

        # ID columns
        if any(kw in col_name for kw in ['id', '_id', 'uuid', 'identifier', 'key']):
            info["semantic"] = "id"

        # Date/time
        elif any(kw in col_name for kw in ['date', 'time', 'created', 'updated', 'timestamp', 'dt', 'published', 'start_date', 'end_date']):
            info["semantic"] = "datetime"
            try:
                pd.to_datetime(non_null.head(100))
                info["type"] = "datetime"
            except (ValueError, TypeError):
                pass

        # Money / financial
        elif any(kw in col_name for kw in ['price', 'cost', 'revenue', 'amount', 'salary', 'wage', 'fee', 'income', 'expense', 'budget', 'profit', 'loss', 'earning', 'rate', 'interest']):
            info["semantic"] = "money"

        # Count / quantity
        elif any(kw in col_name for kw in ['quantity', 'count', 'num', 'number', 'units', 'items', 'stock', 'inventory', 'volume']):
            info["semantic"] = "count"

        # Percentage
        elif any(kw in col_name for kw in ['percent', 'ratio', 'pct', 'share']):
            info["semantic"] = "percentage"

        # Score / rating
        elif any(kw in col_name for kw in ['score', 'rating', 'grade', 'gpa', 'marks', 'marks_']):
            info["semantic"] = "score"

        # Person name
        elif any(kw in col_name for kw in ['name', 'first_name', 'last_name', 'full_name', 'author', 'user', 'customer', 'client', 'patient', 'doctor', 'physician', 'teacher', 'student', 'employee', 'manager', 'seller', 'driver']):
            info["semantic"] = "person"

        # Location
        elif any(kw in col_name for kw in ['region', 'country', 'city', 'state', 'location', 'address', 'zip', 'postal', 'lat', 'lon', 'latitude', 'longitude', 'territory', 'area', 'district', 'province']):
            info["semantic"] = "location"

        # Category / type / status
        elif any(kw in col_name for kw in ['category', 'type', 'status', 'department', 'genre', 'group', 'segment', 'class', 'level', 'tier', 'plan']):
            info["semantic"] = "category"

        # Email
        elif any(kw in col_name for kw in ['email', 'mail']):
            info["semantic"] = "email"

        # Phone
        elif any(kw in col_name for kw in ['phone', 'mobile', 'tel']):
            info["semantic"] = "phone"

        # URL
        elif any(kw in col_name for kw in ['url', 'link', 'website', 'href', 'domain']):
            info["semantic"] = "url"

        # Long text
        elif any(kw in col_name for kw in ['description', 'text', 'content', 'body', 'message', 'summary', 'headline', 'title', 'abstract', 'notes', 'comment', 'review']):
            info["semantic"] = "text"

        # Medical
        elif any(kw in col_name for kw in ['diagnosis', 'treatment', 'medication', 'symptom', 'dosage', 'prescription', 'lab_result', 'blood_pressure', 'heart_rate', 'pulse', 'disease', 'condition', 'procedure']):
            info["semantic"] = "medical_term"

        # Education specific
        elif any(kw in col_name for kw in ['attendance', 'homework', 'assignment', 'semester', 'lecture', 'course', 'school', 'university', 'college', 'exam', 'midterm', 'final_exam', 'study_hours', 'sleep_hours', 'extra_classes', 'parent_education', 'internet_access', 'participation_score', 'overall_score']):
            info["semantic"] = "score"

        # News / publishing
        elif any(kw in col_name for kw in ['headline', 'source', 'publisher', 'journalist', 'edition']):
            info["semantic"] = "text"

        # If it's object type and no semantic matched, check cardinality
        if dtype == 'object' and info["semantic"] == "generic":
            unique_ratio = unique_count / total_count if total_count > 0 else 0
            if unique_ratio < 0.15:  # Fewer than 15% unique values
                info["semantic"] = "category"
            elif unique_ratio > 0.9 and total_count > 50:
                info["semantic"] = "id"

        # Numeric with no semantic: classify by stats
        if info["semantic"] == "generic" and any(t in dtype for t in ['int', 'float', 'double', 'decimal', 'numeric']):
            info["semantic"] = "count"

        return info

    # ──────────────────────────────────────────────────────────────────
    # Dataset type detection
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _detect_dataset_type(columns_info: Dict) -> str:
        """Detect dataset type using weighted keyword scoring."""
        col_names_lower = {k.lower() for k in columns_info.keys()}

        type_keywords = {
            "education": {
                3: {"student", "grade", "midterm", "final_exam", "attendance",
                    "assignment", "exam", "gpa", "school", "university", "college",
                    "teacher", "lecture", "course", "semester", "homework", "class",
                    "study_hours", "participation_score", "overall_score", "sleep_hours",
                    "extra_classes", "parent_education", "internet_access"},
                1: {"score", "pass", "fail"},
            },
            "medical": {
                3: {"patient", "diagnosis", "treatment", "medication", "symptom",
                    "doctor", "physician", "hospital", "disease", "clinical", "dosage",
                    "prescription", "lab_result", "blood_pressure", "heart_rate", "pulse",
                    "procedure", "condition"},
                1: {"age", "gender", "weight", "height", "blood"},
            },
            "sales": {
                3: {"product", "sales", "revenue", "quantity", "price", "order",
                    "customer", "invoice", "discount", "profit", "shipping", "amount",
                    "seller", "store"},
                1: {"region", "total", "amount", "item"},
            },
            "news": {
                3: {"headline", "article", "publisher", "journalist", "edition", "byline"},
                1: {"title", "author", "source", "published", "category", "date"},
            },
            "finance": {
                3: {"stock", "ticker", "exchange", "portfolio", "dividend", "nasdaq",
                    "market_cap", "closing_price", "opening_price", "share",
                    "trade", "bid", "ask"},
                1: {"price", "volume", "market", "return", "change"},
            },
            "hr": {
                3: {"employee", "salary", "hire_date", "performance_review", "manager",
                    "department", "position", "termination", "onboarding"},
                1: {"performance", "attendance"},
            },
            "ecommerce": {
                3: {"cart", "checkout", "sku", "wishlist", "coupon", "order_id", "order_item"},
                1: {"order", "shipping", "product", "quantity", "amount"},
            },
            "iot": {
                3: {"sensor", "device_id", "reading", "humidity", "pressure", "device"},
                1: {"temperature", "timestamp", "device"},
            },
            "logistics": {
                3: {"shipment", "carrier", "tracking", "delivery", "warehouse", "freight"},
                1: {"status", "date", "weight", "origin", "destination"},
            },
        }

        scores: Dict[str, int] = {}
        for dtype, keyword_weights in type_keywords.items():
            score = 0
            for weight, keywords in keyword_weights.items():
                score += weight * len(col_names_lower & keywords)
            if score > 0:
                scores[dtype] = score

        if scores:
            return max(scores, key=scores.get)
        return "generic"

    # ──────────────────────────────────────────────────────────────────
    # Suggested queries generation
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _generate_suggested_queries(columns_info: Dict, dataset_type: str, table_name: str) -> List[str]:
        """Generate 5+ suggested queries based on schema."""
        cats = categorize_columns(columns_info)
        numeric_cols = cats["numeric"]
        category_cols = cats["categorical"]
        date_cols = cats["date"]
        text_cols = cats["text"]

        suggestions = []

        if not numeric_cols and not category_cols and not date_cols:
            return ["Show all records", "Count total records"]

        # Numeric-specific queries
        if numeric_cols:
            col = numeric_cols[0]
            suggestions.append(f"Show top 10 by {col}")

        if category_cols and numeric_cols:
            suggestions.append(f"Total {numeric_cols[0]} by {category_cols[0]}")

        if numeric_cols:
            suggestions.append(f"Average {numeric_cols[0]}")

        if category_cols:
            suggestions.append(f"Distribution of {category_cols[0]}")

        # Date-based queries
        if date_cols and numeric_cols:
            suggestions.append(f"{numeric_cols[0]} trend over time")

        # Dataset-type-specific queries
        if dataset_type == "sales" and category_cols and numeric_cols:
            suggestions.append(f"Bottom 5 {category_cols[0]} by {numeric_cols[0]}")
        elif dataset_type == "medical" and category_cols:
            suggestions.append(f"Patient count by {category_cols[0]}")
        elif dataset_type == "finance" and date_cols:
            suggestions.append(f"{numeric_cols[0]} trend analysis")
        elif dataset_type == "news" and date_cols:
            suggestions.append("Articles by publish date")
        elif dataset_type == "education" and numeric_cols:
            suggestions.append(f"Top 10 students by {numeric_cols[0]}")

        # Count queries
        if category_cols:
            suggestions.append(f"How many records per {category_cols[0]}")

        return suggestions[:6]

    # ──────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────

    async def detect_schema_from_file(self, file_path: str, use_llm: bool = False) -> Dict[str, Any]:
        """Detect schema from a CSV or JSON file (always heuristic)."""
        if not os.path.exists(file_path):
            return {"error": f"File not found: {file_path}"}

        try:
            if file_path.endswith('.csv'):
                df = self._read_csv_robust(file_path, nrows=2000)
            elif file_path.endswith('.json'):
                df = pd.read_json(file_path)
            else:
                return {"error": "Unsupported file format. Use CSV or JSON."}
        except (pd.errors.EmptyDataError, pd.errors.ParserError, ValueError) as e:
            return {"error": f"Failed to read file: {e}"}

        return await self.detect_schema_from_df(df, file_path, use_llm=use_llm)

    async def detect_schema_from_df(
        self, df: pd.DataFrame, source_file: str = None, use_llm: bool = False
    ) -> Dict[str, Any]:
        """Detect schema from a DataFrame. Always produces rich heuristic output.
        When use_llm=True, optionally enriches with LLM-based semantics.
        """
        # Step 1: Detect column types (always heuristic, always works)
        columns_info = {}
        for col in df.columns:
            columns_info[str(col)] = self._detect_column_type(df[col])

        # Step 2: Detect dataset type (always heuristic, always works)
        dataset_type = self._detect_dataset_type(columns_info)

        # Step 3: Generate table name
        table_name = "data_clean"
        if source_file:
            base_name = os.path.basename(source_file).split('.')[0]
            table_name = f"{base_name}_clean"

        # Step 4: Generate suggested queries (always heuristic, always works)
        suggested_queries = self._generate_suggested_queries(columns_info, dataset_type, table_name)

        # Step 5: Build rich schema
        col_cats = categorize_columns(columns_info)

        # Step 6: Generate a human-readable dataset description
        dataset_description = self._generate_dataset_description(dataset_type, columns_info, len(df))

        schema = {
            "source_file": source_file,
            "table_name": table_name,
            "dataset_type": dataset_type,
            "dataset_description": dataset_description,
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "columns": columns_info,
            "column_summary": {
                "numeric": col_cats["numeric"],
                "categorical": col_cats["categorical"],
                "date": col_cats["date"],
                "text": col_cats["text"],
                "id": col_cats["id"],
            },
            "suggested_queries": suggested_queries,
            "detected_at": datetime.now().isoformat(),
            "detection_method": "heuristic",
        }

        # Step 7 (Optional): Enhance with LLM if requested and available
        if use_llm:
            try:
                enriched = await self._enrich_with_llm(schema, df)
                if enriched:
                    schema = enriched
            except Exception as exc:
                logger.warning(f"LLM enrichment failed, using heuristic result: {exc}")

        # Save to cache
        save_schema(schema)

        return schema

    @staticmethod
    def _generate_dataset_description(dataset_type: str, columns_info: Dict, row_count: int) -> str:
        """Generate a human-readable description of the dataset."""
        col_cats = categorize_columns(columns_info)
        numeric_count = len(col_cats["numeric"])
        category_count = len(col_cats["categorical"])
        date_count = len(col_cats["date"])
        text_count = len(col_cats["text"])
        total_cols = len(columns_info)

        type_labels = []
        if numeric_count > 0:
            type_labels.append(f"{numeric_count} numeric")
        if category_count > 0:
            type_labels.append(f"{category_count} categorical")
        if date_count > 0:
            type_labels.append(f"{date_count} date/time")
        if text_count > 0:
            type_labels.append(f"{text_count} text")

        cols_desc = ", ".join(type_labels) if type_labels else "mixed"

        # Get key column names for description
        key_cols = list(columns_info.keys())[:5]
        if len(columns_info) > 5:
            key_cols.append(f"and {len(columns_info) - 5} more")
        cols_str = ", ".join(f"\u2018{c}\u2019" for c in key_cols)

        desc = (
            f"{dataset_type.capitalize()} dataset with {row_count:,} rows and {total_cols} columns "
            f"({cols_desc}). Key columns: {cols_str}."
        )
        return desc

    async def _enrich_with_llm(self, schema: Dict, df: pd.DataFrame) -> Optional[Dict]:
        """Optionally enrich the heuristic schema with LLM-based semantic analysis.
        Returns enriched schema dict, or None if LLM is unavailable.
        """
        try:
            import os
            api_key = os.getenv("OPENAI_API_KEY", "")
            if not api_key:
                return None

            from langchain_openai import ChatOpenAI
            llm = ChatOpenAI(
                model=os.getenv("LLM_MODEL", "gpt-4o-mini"),
                api_key=api_key,
                base_url=os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1"),
                temperature=0.1,
                timeout=30,
            )

            from agent.schemas import SchemaDetectionResult
            chain = llm.with_structured_output(SchemaDetectionResult)

            # Build column summary for LLM
            col_lines = []
            for col_name, col_info in schema["columns"].items():
                dtype = col_info.get("type", "unknown")
                semantic = col_info.get("semantic", "generic")
                sample = col_info.get("sample_values", [])
                col_lines.append(f"  - {col_name} (type={dtype}, semantic={semantic}, samples={sample})")

            prompt = (
                f"Dataset: {schema['dataset_type']}, {schema['row_count']} rows, {schema['column_count']} columns\n"
                f"Table: {schema['table_name']}\n\n"
                f"Columns:\n" + "\n".join(col_lines)
            )

            result: SchemaDetectionResult = await chain.ainvoke([
                {"role": "system", "content": "You are a data schema analyst. Classify this dataset's type and describe each column's semantic meaning."},
                {"role": "user", "content": prompt},
            ])

            # Enrich existing column info with LLM semantics
            for col_sem in result.column_semantics:
                col_name = col_sem.column_name
                if col_name in schema["columns"]:
                    schema["columns"][col_name]["semantic"] = col_sem.semantic_type
                    schema["columns"][col_name]["business_meaning"] = col_sem.business_meaning

            # Update dataset type if LLM is confident
            if result.confidence_score > 0.7:
                schema["dataset_type"] = result.dataset_type
                schema["dataset_description"] = result.dataset_description

            # Merge suggested queries (keep heuristic ones + add LLM ones)
            existing_queries = set(schema.get("suggested_queries", []))
            for q in result.suggested_queries:
                existing_queries.add(q)
            schema["suggested_queries"] = list(existing_queries)[:8]

            schema["detection_method"] = "llm_enhanced"
            schema["llm_confidence"] = result.confidence_score

            return schema

        except Exception as exc:
            logger.warning(f"LLM enrichment skipped: {exc}")
            return None

    def load_schema_cache(self) -> Optional[Dict]:
        """Load schema from cache using shared utility"""
        schema = load_schema()
        return schema if schema else None
