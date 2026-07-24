"""
DataForge AI - Universal Query Agent
Natural Language to SQL for ANY dataset type
"""

import os
import re
import duckdb
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from utils.config import config_manager, DatasetConfig


class UniversalQueryAgent:
    """Agent for converting natural language to SQL for any dataset"""
    
    def __init__(self):
        self.base_path = "/home/z/my-project"
        self.warehouse_path = os.path.join(self.base_path, "warehouse/warehouse.duckdb")
        self.max_retries = 2
        self._current_config: Optional[DatasetConfig] = None
    
    def set_dataset(self, dataset_name: str) -> bool:
        """Set the current dataset configuration"""
        config = config_manager.get_config(dataset_name)
        if config:
            self._current_config = config
            return True
        return False
    
    def get_config(self) -> Optional[DatasetConfig]:
        """Get current dataset configuration"""
        if not self._current_config:
            self._current_config = config_manager.get_current_config()
        return self._current_config
    
    # =========================================================================
    # SQL GENERATION
    # =========================================================================
    
    def _get_schema_description(self, config: DatasetConfig) -> str:
        """Generate human-readable schema description"""
        lines = []
        for col in config.columns:
            desc = f"- {col.name} ({col.data_type})"
            if col.description:
                desc += f": {col.description}"
            lines.append(desc)
        return "\n".join(lines)
    
    def _generate_sql_from_pattern(self, question: str, config: DatasetConfig) -> str:
        """Generate SQL using pattern matching based on dataset type"""
        question_lower = question.lower()
        table = f"{config.name}_clean"
        
        # Check if any predefined pattern matches
        for pattern_name, pattern_sql in config.query_patterns.items():
            # Pattern matching based on question keywords
            if self._pattern_matches(pattern_name, question_lower, config.data_type.value):
                limit = self._extract_limit(question_lower) or 10
                return pattern_sql.format(table=table, limit=limit)
        
        # Fallback: generate SQL dynamically
        return self._generate_dynamic_sql(question_lower, config)
    
    def _pattern_matches(self, pattern_name: str, question: str, data_type: str) -> bool:
        """Check if a pattern name matches the question"""
        pattern_keywords = {
            # Sales patterns
            "top_products": ["top", "product", "best product", "popular product"],
            "by_region": ["by region", "region", "geographic", "location"],
            "monthly_trend": ["monthly", "trend", "over time", "timeline"],
            "total_revenue": ["total revenue", "total sales", "overall"],
            "average_sales": ["average", "avg", "mean"],
            "best_product": ["best", "top 1", "highest", "most successful"],
            "worst_product": ["worst", "bottom", "lowest", "least"],
            
            # News patterns
            "top_sources": ["source", "publisher", "news outlet"],
            "by_category": ["category", "by category", "categories"],
            "most_shared": ["shared", "viral", "most shared"],
            "sentiment_analysis": ["sentiment", "feeling", "tone"],
            "top_authors": ["author", "writer", "journalist"],
            "viral_articles": ["viral", "trending", "popular"],
            "positive_news": ["positive", "good news", "happy"],
            "negative_news": ["negative", "bad news"],
            
            # Medical patterns
            "top_diagnoses": ["diagnosis", "condition", "disease"],
            "by_department": ["department", "unit", "ward"],
            "age_distribution": ["age", "age group", "demographic"],
            "gender_breakdown": ["gender", "sex", "male female"],
            "monthly_admissions": ["admission", "monthly patient"],
            "treatment_outcomes": ["outcome", "result", "recovery"],
            "insurance_analysis": ["insurance", "coverage"],
            "high_cost_patients": ["expensive", "high cost", "costly"],
            "common_medications": ["medication", "drug", "prescription"],
            
            # Financial patterns
            "top_holdings": ["holding", "portfolio", "position"],
            "by_sector": ["sector", "industry", "market segment"],
            "daily_volume": ["volume", "daily", "trading volume"],
            "profitable_trades": ["profit", "gain", "profitable"],
            "broker_comparison": ["broker", "platform"],
            "monthly_performance": ["monthly performance", "month over month"],
            "high_value_transactions": ["large", "big transaction", "high value"]
        }
        
        if pattern_name in pattern_keywords:
            return any(kw in question for kw in pattern_keywords[pattern_name])
        
        return False
    
    def _extract_limit(self, question: str) -> Optional[int]:
        """Extract limit number from question"""
        match = re.search(r'top\s+(\d+)', question)
        if match:
            return int(match.group(1))
        
        # Word-based limits
        if "top one" in question or "the best" in question:
            return 1
        if "top three" in question:
            return 3
        if "top five" in question:
            return 5
        if "top ten" in question:
            return 10
        
        return None
    
    def _generate_dynamic_sql(self, question: str, config: DatasetConfig) -> str:
        """Generate SQL dynamically based on schema analysis"""
        table = f"{config.name}_clean"
        
        # Detect aggregation type
        agg_func = "COUNT"
        if "sum" in question or "total" in question:
            agg_func = "SUM"
        elif "average" in question or "avg" in question:
            agg_func = "AVG"
        elif "max" in question or "highest" in question:
            agg_func = "MAX"
        elif "min" in question or "lowest" in question:
            agg_func = "MIN"
        
        # Detect grouping column
        group_col = None
        for col in config.category_columns:
            if col in question:
                group_col = col
                break
        
        # Detect numeric column for aggregation
        agg_col = "*"
        for col in config.numeric_columns:
            if col in question:
                agg_col = col
                break
        
        # Build SQL
        if group_col:
            sql = f"""
                SELECT {group_col}, {agg_func}({agg_col}) as value
                FROM {table}
                GROUP BY {group_col}
                ORDER BY value DESC
            """
        else:
            sql = f"SELECT {agg_func}({agg_col}) as value FROM {table}"
        
        # Add limit
        limit = self._extract_limit(question)
        if limit and group_col:
            sql += f" LIMIT {limit}"
        
        return sql.strip()
    
    # =========================================================================
    # VALIDATION & EXECUTION
    # =========================================================================
    
    def _validate_sql(self, sql: str) -> bool:
        """Validate SQL for safety"""
        dangerous_keywords = [
            "DROP", "DELETE", "UPDATE", "INSERT", 
            "ALTER", "CREATE", "TRUNCATE", "GRANT", "REVOKE"
        ]
        sql_upper = sql.upper()
        
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                return False
        
        return True
    
    async def execute_sql(self, sql: str) -> Dict[str, Any]:
        """Execute SQL on DuckDB"""
        if not os.path.exists(self.warehouse_path):
            return {"error": "Warehouse not found. Run pipeline first."}
        
        con = duckdb.connect(self.warehouse_path, read_only=True)
        
        try:
            result = con.execute(sql).fetchall()
            columns = [desc[0] for desc in con.description]
            con.close()
            
            return {
                "columns": columns,
                "data": [dict(zip(columns, row)) for row in result],
                "row_count": len(result)
            }
        except Exception as e:
            con.close()
            return {"error": str(e)}
    
    # =========================================================================
    # MAIN PROCESSING
    # =========================================================================
    
    async def process_query(self, question: str, dataset_name: str = None) -> Dict[str, Any]:
        """Process a natural language query"""
        start_time = time.time()
        
        # Set dataset if specified
        if dataset_name:
            self.set_dataset(dataset_name)
        
        config = self.get_config()
        if not config:
            return {
                "status": "error",
                "message": "No dataset configuration. Load a dataset first."
            }
        
        # Generate SQL
        sql = self._generate_sql_from_pattern(question, config)
        
        # Validate
        if not self._validate_sql(sql):
            return {
                "status": "error",
                "message": "Generated SQL contains unsafe operations",
                "sql": sql
            }
        
        # Execute
        result = await self.execute_sql(sql)
        
        execution_time = time.time() - start_time
        
        if "error" in result:
            return {
                "status": "error",
                "sql": sql,
                "message": result["error"],
                "execution_time": execution_time
            }
        
        return {
            "status": "success",
            "question": question,
            "dataset": config.name,
            "dataset_type": config.data_type.value,
            "sql": sql,
            "columns": result["columns"],
            "data": result["data"],
            "row_count": result["row_count"],
            "execution_time": round(execution_time, 3)
        }
    
    async def suggest_queries(self, dataset_name: str = None) -> List[Dict[str, str]]:
        """Suggest queries based on dataset type"""
        if dataset_name:
            self.set_dataset(dataset_name)
        
        config = self.get_config()
        if not config:
            return []
        
        suggestions = []
        
        for pattern_name, pattern_sql in config.query_patterns.items():
            # Generate example question
            question = self._pattern_to_question(pattern_name, config)
            suggestions.append({
                "pattern": pattern_name,
                "question": question,
                "description": f"Query for {pattern_name.replace('_', ' ')}"
            })
        
        return suggestions[:10]  # Return top 10 suggestions
    
    def _pattern_to_question(self, pattern_name: str, config: DatasetConfig) -> str:
        """Convert pattern name to example question"""
        data_type = config.data_type.value
        
        questions = {
            # Sales
            "top_products": f"Top 5 products by revenue",
            "by_region": "Sales by region",
            "monthly_trend": "Monthly sales trend",
            "total_revenue": "Total revenue",
            "average_sales": "Average sales",
            
            # News
            "top_sources": "Top news sources by views",
            "by_category": "Articles by category",
            "most_shared": "Most shared articles",
            "sentiment_analysis": "Sentiment analysis by category",
            "top_authors": "Top authors by views",
            
            # Medical
            "top_diagnoses": "Most common diagnoses",
            "by_department": "Patients by department",
            "age_distribution": "Age group distribution",
            "gender_breakdown": "Patient gender breakdown",
            "monthly_admissions": "Monthly admission trends",
            
            # Financial
            "top_holdings": "Top holdings by value",
            "by_sector": "Transactions by sector",
            "daily_volume": "Daily trading volume",
            "profitable_trades": "Most profitable trades",
        }
        
        return questions.get(pattern_name, f"Show {pattern_name.replace('_', ' ')}")


# Global instance
universal_query_agent = UniversalQueryAgent()
