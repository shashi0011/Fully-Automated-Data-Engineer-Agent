"""
DataForge AI - Schema Detector
Auto-detects schema from any CSV/JSON file and generates metadata
"""
"""
DataForge AI - Schema Detector
Auto-detects schema from any CSV/JSON file and generates metadata.
Uses LLM for intelligent classification when available, falls back to heuristic keywords.
"""

import os
import re
import json
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
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
    """Auto-detects schema from data files"""

    def __init__(self):
        pass

    async def _detect_with_llm(self, df: pd.DataFrame, source_file: str = None) -> Optional[Dict]:
        """Use LLM to classify dataset type, column semantics, and generate suggestions.
        
        Returns None if LLM is unavailable, so caller falls back to heuristic.
        """
        try:
            from agent.tools.llm_agent import LLMAgent
            from agent.schemas import SchemaDetectionResult

            llm = LLMAgent()
            if not llm._available or llm.llm_analysis is None:
                logger.info("[SchemaDetector] LLM not available, using heuristic fallback")
                return None

            # Build column info for LLM
            col_lines = []
            for col in df.columns:
                dtype = str(df[col].dtype)
                non_null = df[col].dropna()
                sample_vals = non_null.head(3).tolist()
                # Convert numpy types
                sample_str = ", ".join(
                    str(v) if not (hasattr(v, 'item')) else str(v.item())
                    for v in sample_vals
                )
                col_lines.append(
                    f"  - {col} (type={dtype}, sample=[{sample_str}], unique={len(non_null.unique())})"
                )

            # Take 5 sample rows
            sample_rows = df.head(5).to_dict(orient="records")
            sample_str = json.dumps(sample_rows, indent=2, default=str)

            col_desc = "\n".join(col_lines)

            system_prompt = (
                "You are an expert data analyst. Analyze the provided dataset columns "
                "and sample data to:\n"
                "1. Classify the dataset type (education, sales, medical, finance, hr, "
                "news, ecommerce, iot, logistics, generic)\n"
                "2. Assign semantic types to every column\n"
                "3. Describe the dataset in one sentence\n"
                "4. Suggest 5 relevant queries for exploring this dataset\n\n"
                "IMPORTANT:\n"
                "- Look at ALL columns TOGETHER to understand context\n"
                "- 'gender' alone does NOT mean medical — check for actual medical terms\n"
                "- 'score', 'grade', 'attendance', 'exam' = education\n"
                "- 'patient', 'diagnosis', 'prescription' = medical\n"
                "- Consider the COMBINATION of columns, not individual ones\n"
                "- Only assign 'medical_term' semantic to actual medical vocabulary\n\n"
                "Respond ONLY with valid JSON matching the SchemaDetectionResult schema."
            )

            user_prompt = (
                f"Analyze this dataset:\n\n"
                f"Columns ({len(df.columns)} total):\n{col_desc}\n\n"
                f"Sample Data (first 5 rows):\n{sample_str}\n\n"
                f"Classify this dataset and assign semantic types."
            )

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]

            result: SchemaDetectionResult = await llm._invoke_llm_with_fallback(
                llm.llm_analysis,
                llm.llm_analysis_fallback,
                messages,
                structured_output_type=SchemaDetectionResult,
            )

            logger.info(
                f"[SchemaDetector] LLM classified as '{result.dataset_type}' "
                f"(confidence={result.confidence_score})"
            )

            return result.model_dump()

        except Exception as e:
            logger.warning(f"[SchemaDetector] LLM detection failed: {e}")
            return None

    def _convert_to_native(self, value):
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

    def _detect_column_type(self, series: pd.Series) -> Dict[str, Any]:
        """Detect the semantic type of a column"""
        col_name = str(series.name).lower()
        non_null = series.dropna()

        if len(non_null) == 0:
            return {"type": "unknown", "semantic": "unknown", "nullable": True}

        # Get dtype info
        dtype = str(series.dtype)
        # Convert numpy types to Python native types for JSON serialization
        sample_values = [self._convert_to_native(v) for v in non_null.head(5).tolist()]

        # Semantic type detection based on column name and values
        semantic_info = {
            "type": dtype,
            "semantic": "generic",
            "nullable": bool(series.isnull().any())  # Convert numpy.bool_ to Python bool
        }

        # ID columns
        if any(kw in col_name for kw in ['id', '_id', 'uuid', 'key']):
            semantic_info["semantic"] = "id"

        # Date/time columns
        elif any(kw in col_name for kw in ['date', 'time', 'created', 'updated', 'timestamp', 'dt']):
            semantic_info["semantic"] = "datetime"
            # Try to parse as date
            try:
                pd.to_datetime(non_null.head(100))
                semantic_info["type"] = "datetime"
            except (ValueError, TypeError):
                pass

        # Numeric columns
        elif 'price' in col_name or 'cost' in col_name or 'revenue' in col_name or 'amount' in col_name:
            semantic_info["semantic"] = "money"
        elif 'quantity' in col_name or 'count' in col_name or 'num' in col_name:
            semantic_info["semantic"] = "count"
        elif 'rate' in col_name or 'ratio' in col_name or 'percent' in col_name:
            semantic_info["semantic"] = "percentage"
        elif 'score' in col_name or 'rating' in col_name:
            semantic_info["semantic"] = "score"

        # Text columns with semantic meaning
        elif any(kw in col_name for kw in ['name', 'title', 'product', 'item']):
            semantic_info["semantic"] = "name"
        elif any(kw in col_name for kw in ['region', 'country', 'city', 'state', 'location']):
            semantic_info["semantic"] = "location"
        elif any(kw in col_name for kw in ['category', 'type', 'status', 'department']):
            semantic_info["semantic"] = "category"
        elif any(kw in col_name for kw in ['email', 'mail']):
            semantic_info["semantic"] = "email"
        elif any(kw in col_name for kw in ['phone', 'mobile', 'tel']):
            semantic_info["semantic"] = "phone"
        elif any(kw in col_name for kw in ['url', 'link', 'website']):
            semantic_info["semantic"] = "url"
        elif any(kw in col_name for kw in ['description', 'text', 'content', 'body', 'message']):
            semantic_info["semantic"] = "text"
        elif any(kw in col_name for kw in ['author', 'user', 'customer', 'client', 'patient']):
            semantic_info["semantic"] = "person"

        # News-specific
        elif any(kw in col_name for kw in ['headline', 'source', 'publisher']):
            semantic_info["semantic"] = "text"

        # Medical-specific
        elif any(kw in col_name for kw in ['diagnosis', 'treatment', 'medication', 'symptom']):
            semantic_info["semantic"] = "medical_term"
        elif any(kw in col_name for kw in ['patient', 'doctor', 'physician']):
            semantic_info["semantic"] = "person"

        # If it's object type, check if it looks like a category
        if dtype == 'object' and semantic_info["semantic"] == "generic":
            unique_ratio = len(non_null.unique()) / len(non_null)
            if unique_ratio < 0.1:  # Less than 10% unique values
                semantic_info["semantic"] = "category"

        semantic_info["sample_values"] = sample_values[:3]
        semantic_info["unique_count"] = int(len(non_null.unique()))
        semantic_info["total_count"] = int(len(non_null))

        return semantic_info

    async def detect_schema_from_file(self, file_path: str, use_llm: bool = True) -> Dict[str, Any]:
        """Detect schema from a CSV or JSON file"""
        if not os.path.exists(file_path):
            return {"error": f"File not found: {file_path}"}

        # Read file
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, nrows=1000)  # Sample for detection
            elif file_path.endswith('.json'):
                df = pd.read_json(file_path)
            else:
                return {"error": "Unsupported file format. Use CSV or JSON."}
        except (pd.errors.EmptyDataError, pd.errors.ParserError, ValueError) as e:
            return {"error": f"Failed to read file: {e}"}

        return await self.detect_schema_from_df(df, file_path, use_llm=use_llm)

    async def detect_schema_from_df(
        self, df: pd.DataFrame, source_file: str = None, use_llm: bool = True
    ) -> Dict[str, Any]:
        """Detect schema from a DataFrame.
        
        If use_llm=True and LLM is available, uses intelligent classification.
        Falls back to heuristic keyword matching otherwise.
        """
        # Step 1: Always detect basic column types (dtype, nullable, sample values)
        columns_info = {}
        for col in df.columns:
            columns_info[str(col)] = self._detect_column_type(df[col])

        # Step 2: Try LLM classification
        llm_result = None
        if use_llm:
            llm_result = await self._detect_with_llm(df, source_file)

        # Step 3: Determine dataset type
        if llm_result and llm_result.get("confidence_score", 0) >= 0.3:
            # Use LLM result — overwrite semantic types and dataset type
            dataset_type = llm_result["dataset_type"]
            dataset_description = llm_result.get("dataset_description", "")

            # Override column semantic types with LLM's understanding
            for col_semantic in llm_result.get("column_semantics", []):
                col_name = col_semantic["column_name"]
                if col_name in columns_info:
                    columns_info[col_name]["semantic"] = col_semantic["semantic_type"]
                    columns_info[col_name]["business_meaning"] = col_semantic.get("business_meaning", "")

            # Use LLM suggested queries
            suggested_queries = llm_result.get("suggested_queries", [])
        else:
            # Fall back to heuristic
            dataset_type = self._detect_dataset_type(columns_info)
            dataset_description = ""
            suggested_queries = self._generate_suggested_queries(columns_info, dataset_type, table_name)

        # Step 4: Generate table name
        table_name = "data_clean"
        if source_file:
            base_name = os.path.basename(source_file).split('.')[0]
            table_name = f"{base_name}_clean"

        # Step 5: Build schema
        schema = {
            "source_file": source_file,
            "table_name": table_name,
            "dataset_type": dataset_type,
            "dataset_description": dataset_description,
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "columns": columns_info,
            "detected_at": datetime.now().isoformat(),
            "suggested_queries": suggested_queries,
            "llm_used": llm_result is not None,
            "detection_method": "llm" if llm_result else "heuristic",
        }

        # Save to cache using shared utility (with thread-safe locking)
        save_schema(schema)

        return schema
    def _detect_dataset_type(self, columns_info: Dict) -> str:
        """Detect the type of dataset using weighted scoring (education-aware, no false medical)"""
        col_names_lower = {k.lower() for k in columns_info.keys()}

        # Strong keywords = 3 points, Weak keywords = 1 point
        # Generic terms like age, gender, name only give 1 point (never trigger false positives)
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
                    "prescription", "lab_result", "blood_pressure", "heart_rate", "pulse"},
                1: {"age", "gender", "weight", "height", "blood"},
            },
            "sales": {
                3: {"product", "sales", "revenue", "quantity", "price", "order",
                    "customer", "invoice", "discount", "profit", "shipping"},
                1: {"region", "total", "amount"},
            },
            "news": {
                3: {"headline", "article", "publisher", "journalist", "edition"},
                1: {"title", "author", "source", "published", "category", "date"},
            },
            "finance": {
                3: {"stock", "ticker", "exchange", "portfolio", "dividend", "nasdaq",
                    "market_cap", "closing_price", "opening_price"},
                1: {"price", "volume", "market", "return"},
            },
            "hr": {
                3: {"employee", "salary", "hire_date", "performance_review", "manager"},
                1: {"department", "position", "performance", "attendance"},
            },
            "ecommerce": {
                3: {"cart", "checkout", "sku", "wishlist", "coupon"},
                1: {"order", "shipping", "product", "quantity"},
            },
            "iot": {
                3: {"sensor", "device_id", "reading", "humidity", "pressure"},
                1: {"temperature", "timestamp", "device"},
            },
        }

        # Score each type
        scores: Dict[str, int] = {}
        for dtype, keyword_weights in type_keywords.items():
            score = 0
            for weight, keywords in keyword_weights.items():
                score += weight * len(col_names_lower & keywords)
            if score > 0:
                scores[dtype] = score

        # Return the highest-scoring type
        if scores:
            return max(scores, key=scores.get)

        return "generic"

    def _generate_suggested_queries(self, columns_info: Dict, dataset_type: str, table_name: str) -> List[str]:
        """Generate suggested queries based on schema using shared categorize_columns"""
        suggestions = []

        cats = categorize_columns(columns_info)
        numeric_cols = cats["numeric"]
        category_cols = cats["categorical"]
        date_cols = cats["date"]

        if dataset_type == "sales":
            if category_cols and numeric_cols:
                suggestions.append(f"Show total {numeric_cols[0]} by {category_cols[0]}")
            if numeric_cols:
                suggestions.append(f"Top 5 by {numeric_cols[0]}")
            if date_cols and numeric_cols:
                suggestions.append(f"Monthly trend of {numeric_cols[0]}")

        elif dataset_type == "news":
            if category_cols:
                suggestions.append(f"Count articles by {category_cols[0]}")
            if date_cols:
                suggestions.append("Articles by date")
            suggestions.append("Most recent articles")

        elif dataset_type == "medical":
            if category_cols:
                suggestions.append(f"Patients by {category_cols[0]}")
            if numeric_cols:
                suggestions.append(f"Average {numeric_cols[0]} by patient group")
            suggestions.append("Diagnosis distribution")

        elif dataset_type == "education":
            if category_cols:
                suggestions.append(f"Average scores by {category_cols[0]}")
            if numeric_cols:
                suggestions.append(f"Top students by {numeric_cols[0]}")
            if category_cols and numeric_cols:
                suggestions.append(f"Compare {numeric_cols[0]} across {category_cols[0]}")
            suggestions.append("Grade distribution")
            suggestions.append("Students at risk of failing")

        elif dataset_type == "finance":
            if numeric_cols:
                suggestions.append(f"Average {numeric_cols[0]}")
            if date_cols:
                suggestions.append(f"{numeric_cols[0]} trend over time")

        else:
            # Generic suggestions
            if numeric_cols:
                suggestions.append(f"Sum of {numeric_cols[0]}")
                suggestions.append(f"Average {numeric_cols[0]}")
            if category_cols:
                suggestions.append(f"Group by {category_cols[0]}")
            suggestions.append("Show all records")

        return suggestions[:5]

    def load_schema_cache(self) -> Optional[Dict]:
        """Load schema from cache using shared utility"""
        schema = load_schema()
        return schema if schema else None

    def generate_sql_suggestions(self, question: str, schema: Dict) -> str:
        """Generate SQL based on question and detected schema with safe identifiers"""
        question_lower = question.lower()
        columns = schema.get('columns', {})
        table_name = schema.get('table_name', 'data_clean')

        if not validate_identifier(table_name):
            return f"SELECT * FROM {quote_identifier('data_clean')} LIMIT 20"

        # Use shared categorize_columns
        cats = categorize_columns(columns)
        numeric_cols = cats["numeric"]
        category_cols = cats["categorical"]
        date_cols = cats["date"]

        # Quote table name once
        quoted_table = quote_identifier(table_name)

        # Extract numbers from question
        limit_match = re.search(r'top\s+(\d+)|first\s+(\d+)', question_lower)
        limit = int(limit_match.group(1) or limit_match.group(2)) if limit_match else 10

        # Detect aggregation type
        agg_func = "SUM"
        if 'average' in question_lower or 'avg' in question_lower:
            agg_func = "AVG"
        elif 'count' in question_lower or 'how many' in question_lower:
            agg_func = "COUNT"
        elif 'max' in question_lower or 'highest' in question_lower or 'most' in question_lower:
            agg_func = "MAX"
        elif 'min' in question_lower or 'lowest' in question_lower or 'least' in question_lower:
            agg_func = "MIN"

        # Find matching column from question
        def find_matching_column(question: str, col_list: List[str]) -> Optional[str]:
            for col in col_list:
                if col.lower() in question:
                    return col
            return col_list[0] if col_list else None

        # Generate SQL based on patterns with quoted identifiers

        # Top N pattern
        if 'top' in question_lower or 'best' in question_lower or 'highest' in question_lower:
            target_col = find_matching_column(question_lower, numeric_cols) or (numeric_cols[0] if numeric_cols else None)
            group_col = find_matching_column(question_lower, category_cols) or (category_cols[0] if category_cols else None)

            if target_col and group_col:
                return f"SELECT {quote_identifier(group_col)}, {agg_func}({quote_identifier(target_col)}) as total_{target_col} FROM {quoted_table} GROUP BY {quote_identifier(group_col)} ORDER BY total_{target_col} DESC LIMIT {limit}"
            elif group_col:
                return f"SELECT {quote_identifier(group_col)}, COUNT(*) as count FROM {quoted_table} GROUP BY {quote_identifier(group_col)} ORDER BY count DESC LIMIT {limit}"

        # Bottom N pattern
        if 'bottom' in question_lower or 'worst' in question_lower or 'lowest' in question_lower:
            target_col = find_matching_column(question_lower, numeric_cols) or (numeric_cols[0] if numeric_cols else None)
            group_col = find_matching_column(question_lower, category_cols) or (category_cols[0] if category_cols else None)

            if target_col and group_col:
                return f"SELECT {quote_identifier(group_col)}, {agg_func}({quote_identifier(target_col)}) as total_{target_col} FROM {quoted_table} GROUP BY {quote_identifier(group_col)} ORDER BY total_{target_col} ASC LIMIT {limit}"

        # Trend over time
        if 'trend' in question_lower or 'over time' in question_lower or 'monthly' in question_lower or 'daily' in question_lower:
            target_col = find_matching_column(question_lower, numeric_cols) or (numeric_cols[0] if numeric_cols else None)
            date_col = date_cols[0] if date_cols else None

            if date_col and target_col:
                return f"SELECT strftime({quote_identifier(date_col)}, '%Y-%m') as period, {agg_func}({quote_identifier(target_col)}) as value FROM {quoted_table} GROUP BY period ORDER BY period"
            elif date_col:
                return f"SELECT strftime({quote_identifier(date_col)}, '%Y-%m') as period, COUNT(*) as count FROM {quoted_table} GROUP BY period ORDER BY period"

        # Group by pattern
        if 'by' in question_lower:
            group_col = find_matching_column(question_lower, category_cols)
            target_col = find_matching_column(question_lower, numeric_cols)

            if group_col and target_col:
                return f"SELECT {quote_identifier(group_col)}, {agg_func}({quote_identifier(target_col)}) as {agg_func.lower()}_{target_col} FROM {quoted_table} GROUP BY {quote_identifier(group_col)} ORDER BY {agg_func.lower()}_{target_col} DESC"
            elif group_col:
                return f"SELECT {quote_identifier(group_col)}, COUNT(*) as count FROM {quoted_table} GROUP BY {quote_identifier(group_col)} ORDER BY count DESC"

        # Total/sum pattern
        if 'total' in question_lower or 'sum' in question_lower:
            target_col = find_matching_column(question_lower, numeric_cols)
            if target_col:
                return f"SELECT SUM({quote_identifier(target_col)}) as total_{target_col}, COUNT(*) as record_count FROM {quoted_table}"

        # Average pattern
        if 'average' in question_lower or 'avg' in question_lower:
            target_col = find_matching_column(question_lower, numeric_cols)
            if target_col:
                return f"SELECT AVG({quote_identifier(target_col)}) as avg_{target_col} FROM {quoted_table}"

        # Count pattern
        if 'count' in question_lower or 'how many' in question_lower:
            group_col = find_matching_column(question_lower, category_cols)
            if group_col:
                return f"SELECT {quote_identifier(group_col)}, COUNT(*) as count FROM {quoted_table} GROUP BY {quote_identifier(group_col)} ORDER BY count DESC"
            return f"SELECT COUNT(*) as total_count FROM {quoted_table}"

        # Show all / list pattern
        if 'show' in question_lower or 'list' in question_lower or 'all' in question_lower:
            return f"SELECT * FROM {quoted_table} LIMIT {limit}"

        # Default: show summary
        if numeric_cols and category_cols:
            return f"SELECT {quote_identifier(category_cols[0])}, {agg_func}({quote_identifier(numeric_cols[0])}) as total FROM {quoted_table} GROUP BY {quote_identifier(category_cols[0])} ORDER BY total DESC LIMIT 20"
        elif category_cols:
            return f"SELECT {quote_identifier(category_cols[0])}, COUNT(*) as count FROM {quoted_table} GROUP BY {quote_identifier(category_cols[0])} ORDER BY count DESC LIMIT 20"
        else:
            return f"SELECT * FROM {quoted_table} LIMIT 20"

    def get_transformation_sql(self, schema: Dict) -> str:
        """Generate transformation SQL based on detected schema with safe identifiers"""
        columns = schema.get('columns', {})
        table_name = schema.get('table_name', 'data_clean')

        if not validate_identifier(table_name):
            return "-- Error: invalid table name"

        quoted_table = quote_identifier(table_name)
        select_parts = []

        for col_name, col_info in columns.items():
            # Skip columns with unsafe names
            if not validate_identifier(col_name):
                continue

            dtype = col_info.get('type', '')
            semantic = col_info.get('semantic', '')
            quoted_col = quote_identifier(col_name)

            # Generate appropriate CAST based on type
            if 'int' in dtype:
                select_parts.append(f"CAST({quoted_col} AS INTEGER) as {quoted_col}")
            elif 'float' in dtype or 'double' in dtype or semantic == 'money':
                select_parts.append(f"CAST({quoted_col} AS DOUBLE) as {quoted_col}")
            elif semantic == 'datetime' or 'date' in dtype:
                select_parts.append(f"CAST({quoted_col} AS DATE) as {quoted_col}")
            else:
                select_parts.append(quoted_col)

        # Build NOT NULL conditions for key columns
        not_null_cols = [
            k for k, v in columns.items()
            if v.get('semantic') in ['id', 'name', 'category']
            and not v.get('nullable', True)
            and validate_identifier(k)
        ]

        where_parts = [f"{quote_identifier(col)} IS NOT NULL" for col in not_null_cols]
        where_clause = " AND ".join(where_parts) if where_parts else "1=1"

        return f"""
            CREATE OR REPLACE TABLE {quoted_table} AS
            SELECT
                {', '.join(select_parts)}
            FROM "data_raw"
            WHERE {where_clause}
        """
