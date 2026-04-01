"""
DataForge AI - Universal Dataset Configuration System
Supports ANY dataset type: sales, news, medical, financial, etc.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import json
import os


class DataType(Enum):
    """Supported data types"""
    SALES = "sales"
    NEWS = "news"
    MEDICAL = "medical"
    FINANCIAL = "financial"
    CUSTOM = "custom"


@dataclass
class ColumnConfig:
    """Configuration for a single column"""
    name: str
    data_type: str  # string, integer, float, date, boolean, text
    description: str = ""
    is_required: bool = True
    is_unique: bool = False
    default_value: Any = None
    validation_rules: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "description": self.description,
            "is_required": self.is_required,
            "is_unique": self.is_unique,
            "default_value": self.default_value,
            "validation_rules": self.validation_rules
        }


@dataclass
class DatasetConfig:
    """Complete configuration for a dataset"""
    name: str
    data_type: DataType
    description: str
    source_file: str
    
    # Column definitions
    columns: List[ColumnConfig]
    
    # Data processing rules
    cleaning_rules: Dict[str, Any] = field(default_factory=dict)
    transformation_rules: Dict[str, Any] = field(default_factory=dict)
    aggregation_rules: Dict[str, Any] = field(default_factory=dict)
    
    # Query patterns for this data type
    query_patterns: Dict[str, str] = field(default_factory=dict)
    
    # Report configuration
    report_columns: List[str] = field(default_factory=list)
    report_aggregations: List[str] = field(default_factory=list)
    
    # Metadata
    primary_key: str = ""
    date_column: str = ""
    category_columns: List[str] = field(default_factory=list)
    numeric_columns: List[str] = field(default_factory=list)
    text_columns: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "data_type": self.data_type.value,
            "description": self.description,
            "source_file": self.source_file,
            "columns": [col.to_dict() for col in self.columns],
            "cleaning_rules": self.cleaning_rules,
            "transformation_rules": self.transformation_rules,
            "aggregation_rules": self.aggregation_rules,
            "query_patterns": self.query_patterns,
            "report_columns": self.report_columns,
            "report_aggregations": self.report_aggregations,
            "primary_key": self.primary_key,
            "date_column": self.date_column,
            "category_columns": self.category_columns,
            "numeric_columns": self.numeric_columns,
            "text_columns": self.text_columns
        }


# ============================================================================
# PRE-BUILT DATASET CONFIGURATIONS
# ============================================================================

DATASET_CONFIGS: Dict[str, DatasetConfig] = {}


# ----------------------------------------------------------------------------
# SALES DATA CONFIGURATION
# ----------------------------------------------------------------------------
DATASET_CONFIGS["sales"] = DatasetConfig(
    name="sales",
    data_type=DataType.SALES,
    description="E-commerce sales transaction data",
    source_file="data/raw/sales.csv",
    
    columns=[
        ColumnConfig("product", "string", "Product name", is_required=True),
        ColumnConfig("region", "string", "Geographic region", is_required=True),
        ColumnConfig("date", "date", "Transaction date", is_required=True),
        ColumnConfig("sales", "float", "Unit price", is_required=True, validation_rules=["positive"]),
        ColumnConfig("quantity", "integer", "Units sold", is_required=True, validation_rules=["positive"]),
    ],
    
    cleaning_rules={
        "remove_nulls": ["product", "region"],
        "positive_values": ["sales", "quantity"],
        "date_format": "YYYY-MM-DD"
    },
    
    transformation_rules={
        "calculated_columns": {
            "total_revenue": "sales * quantity"
        },
        "type_casts": {
            "date": "DATE",
            "sales": "DOUBLE",
            "quantity": "INTEGER"
        }
    },
    
    aggregation_rules={
        "group_by": ["product", "region"],
        "metrics": ["SUM(quantity)", "SUM(total_revenue)", "AVG(sales)", "COUNT(*)"]
    },
    
    query_patterns={
        "top_products": "SELECT product, SUM(total_revenue) as revenue FROM {table} GROUP BY product ORDER BY revenue DESC LIMIT {limit}",
        "by_region": "SELECT region, SUM(total_revenue) as revenue FROM {table} GROUP BY region ORDER BY revenue DESC",
        "monthly_trend": "SELECT strftime(date, '%Y-%m') as month, SUM(total_revenue) as revenue FROM {table} GROUP BY month ORDER BY month",
        "total_revenue": "SELECT SUM(total_revenue) as total FROM {table}",
        "average_sales": "SELECT AVG(sales) as avg_sales FROM {table}",
        "best_product": "SELECT product, SUM(total_revenue) as revenue FROM {table} GROUP BY product ORDER BY revenue DESC LIMIT 1",
        "worst_product": "SELECT product, SUM(total_revenue) as revenue FROM {table} GROUP BY product ORDER BY revenue ASC LIMIT 1"
    },
    
    report_columns=["product", "region", "transaction_count", "total_quantity", "total_revenue", "avg_sales_price"],
    report_aggregations=["SUM", "COUNT", "AVG"],
    
    primary_key="id",
    date_column="date",
    category_columns=["product", "region"],
    numeric_columns=["sales", "quantity", "total_revenue"],
    text_columns=["product"]
)


# ----------------------------------------------------------------------------
# NEWS DATA CONFIGURATION
# ----------------------------------------------------------------------------
DATASET_CONFIGS["news"] = DatasetConfig(
    name="news",
    data_type=DataType.NEWS,
    description="News articles and sentiment analysis data",
    source_file="data/raw/news.csv",
    
    columns=[
        ColumnConfig("title", "text", "Article headline", is_required=True),
        ColumnConfig("source", "string", "News source/publisher", is_required=True),
        ColumnConfig("category", "string", "News category (politics, sports, tech, etc.)", is_required=True),
        ColumnConfig("published_date", "date", "Publication date", is_required=True),
        ColumnConfig("author", "string", "Article author", is_required=False),
        ColumnConfig("content", "text", "Full article content", is_required=True),
        ColumnConfig("sentiment", "float", "Sentiment score (-1 to 1)", is_required=False, validation_rules=["range_-1_1"]),
        ColumnConfig("word_count", "integer", "Number of words", is_required=False, validation_rules=["positive"]),
        ColumnConfig("views", "integer", "Page views", is_required=False, validation_rules=["positive"]),
        ColumnConfig("shares", "integer", "Social shares", is_required=False, validation_rules=["positive"]),
    ],
    
    cleaning_rules={
        "remove_nulls": ["title", "content", "source"],
        "text_clean": ["title", "content"],
        "date_format": "YYYY-MM-DD",
        "sentiment_range": [-1, 1]
    },
    
    transformation_rules={
        "calculated_columns": {
            "engagement_score": "views + shares * 2",
            "reading_time_minutes": "word_count / 200"
        },
        "text_processing": {
            "lowercase": ["category", "source"],
            "trim": ["title", "author"]
        }
    },
    
    aggregation_rules={
        "group_by": ["category", "source"],
        "metrics": ["COUNT(*)", "AVG(sentiment)", "SUM(views)", "SUM(shares)"]
    },
    
    query_patterns={
        "top_sources": "SELECT source, COUNT(*) as article_count, SUM(views) as total_views FROM {table} GROUP BY source ORDER BY total_views DESC LIMIT {limit}",
        "by_category": "SELECT category, COUNT(*) as count, AVG(sentiment) as avg_sentiment FROM {table} GROUP BY category ORDER BY count DESC",
        "daily_trend": "SELECT published_date as date, COUNT(*) as articles, AVG(sentiment) as avg_sentiment FROM {table} GROUP BY date ORDER BY date",
        "most_shared": "SELECT title, source, shares, sentiment FROM {table} ORDER BY shares DESC LIMIT {limit}",
        "sentiment_analysis": "SELECT category, AVG(sentiment) as avg_sentiment, COUNT(*) as count FROM {table} GROUP BY category ORDER BY avg_sentiment DESC",
        "top_authors": "SELECT author, COUNT(*) as articles, SUM(views) as total_views FROM {table} WHERE author IS NOT NULL GROUP BY author ORDER BY total_views DESC LIMIT {limit}",
        "viral_articles": "SELECT title, source, views, shares, engagement_score FROM {table} WHERE shares > 1000 ORDER BY engagement_score DESC LIMIT {limit}",
        "positive_news": "SELECT title, source, sentiment FROM {table} WHERE sentiment > 0.5 ORDER BY sentiment DESC LIMIT {limit}",
        "negative_news": "SELECT title, source, sentiment FROM {table} WHERE sentiment < -0.3 ORDER BY sentiment ASC LIMIT {limit}",
        "category_breakdown": "SELECT category, COUNT(*) as count, AVG(views) as avg_views, AVG(shares) as avg_shares FROM {table} GROUP BY category"
    },
    
    report_columns=["category", "source", "article_count", "total_views", "total_shares", "avg_sentiment", "engagement_score"],
    report_aggregations=["COUNT", "SUM", "AVG"],
    
    primary_key="id",
    date_column="published_date",
    category_columns=["category", "source", "author"],
    numeric_columns=["sentiment", "word_count", "views", "shares", "engagement_score"],
    text_columns=["title", "content", "author"]
)


# ----------------------------------------------------------------------------
# MEDICAL DATA CONFIGURATION
# ----------------------------------------------------------------------------
DATASET_CONFIGS["medical"] = DatasetConfig(
    name="medical",
    data_type=DataType.MEDICAL,
    description="Medical/Healthcare patient records and diagnostics",
    source_file="data/raw/medical.csv",
    
    columns=[
        ColumnConfig("patient_id", "string", "Unique patient identifier", is_required=True, is_unique=True),
        ColumnConfig("age", "integer", "Patient age", is_required=True, validation_rules=["positive", "range_0_150"]),
        ColumnConfig("gender", "string", "Patient gender", is_required=True),
        ColumnConfig("diagnosis", "string", "Primary diagnosis", is_required=True),
        ColumnConfig("department", "string", "Hospital department", is_required=True),
        ColumnConfig("admission_date", "date", "Date of admission", is_required=True),
        ColumnConfig("discharge_date", "date", "Date of discharge", is_required=False),
        ColumnConfig("treatment", "string", "Treatment type", is_required=False),
        ColumnConfig("medication", "string", "Prescribed medication", is_required=False),
        ColumnConfig("cost", "float", "Treatment cost", is_required=False, validation_rules=["positive"]),
        ColumnConfig("insurance", "string", "Insurance type", is_required=False),
        ColumnConfig("outcome", "string", "Treatment outcome", is_required=False),
        ColumnConfig("lab_results", "string", "Lab test results", is_required=False),
        ColumnConfig("vitals_bp", "string", "Blood pressure", is_required=False),
        ColumnConfig("vitals_temp", "float", "Temperature (F)", is_required=False),
    ],
    
    cleaning_rules={
        "remove_nulls": ["patient_id", "diagnosis"],
        "valid_age": {"min": 0, "max": 150},
        "date_format": "YYYY-MM-DD",
        "valid_genders": ["M", "F", "Male", "Female", "Other"],
        "positive_values": ["cost"]
    },
    
    transformation_rules={
        "calculated_columns": {
            "length_of_stay": "DATEDIFF(day, admission_date, COALESCE(discharge_date, CURRENT_DATE))",
            "age_group": "CASE WHEN age < 18 THEN 'Child' WHEN age < 65 THEN 'Adult' ELSE 'Senior' END",
            "cost_category": "CASE WHEN cost < 1000 THEN 'Low' WHEN cost < 10000 THEN 'Medium' ELSE 'High' END"
        },
        "standardize": {
            "gender": {"M": "Male", "F": "Female"}
        }
    },
    
    aggregation_rules={
        "group_by": ["diagnosis", "department", "age_group"],
        "metrics": ["COUNT(*)", "AVG(cost)", "AVG(length_of_stay)"]
    },
    
    query_patterns={
        "top_diagnoses": "SELECT diagnosis, COUNT(*) as patient_count, AVG(cost) as avg_cost FROM {table} GROUP BY diagnosis ORDER BY patient_count DESC LIMIT {limit}",
        "by_department": "SELECT department, COUNT(*) as patients, AVG(cost) as avg_cost, AVG(length_of_stay) as avg_stay FROM {table} GROUP BY department ORDER BY patients DESC",
        "age_distribution": "SELECT age_group, COUNT(*) as count, AVG(cost) as avg_cost FROM {table} GROUP BY age_group ORDER BY age_group",
        "gender_breakdown": "SELECT gender, COUNT(*) as count, AVG(age) as avg_age, AVG(cost) as avg_cost FROM {table} GROUP BY gender",
        "monthly_admissions": "SELECT strftime(admission_date, '%Y-%m') as month, COUNT(*) as admissions, AVG(cost) as avg_cost FROM {table} GROUP BY month ORDER BY month",
        "treatment_outcomes": "SELECT outcome, COUNT(*) as count, AVG(cost) as avg_cost FROM {table} WHERE outcome IS NOT NULL GROUP BY outcome ORDER BY count DESC",
        "insurance_analysis": "SELECT insurance, COUNT(*) as patients, AVG(cost) as avg_cost FROM {table} WHERE insurance IS NOT NULL GROUP BY insurance ORDER BY avg_cost DESC",
        "high_cost_patients": "SELECT patient_id, diagnosis, cost, length_of_stay FROM {table} WHERE cost > 10000 ORDER BY cost DESC LIMIT {limit}",
        "common_medications": "SELECT medication, COUNT(*) as prescriptions, AVG(cost) as avg_cost FROM {table} WHERE medication IS NOT NULL GROUP BY medication ORDER BY prescriptions DESC LIMIT {limit}",
        "readmission_risk": "SELECT patient_id, diagnosis, COUNT(*) as visits, SUM(cost) as total_cost FROM {table} GROUP BY patient_id, diagnosis HAVING COUNT(*) > 1 ORDER BY visits DESC"
    },
    
    report_columns=["diagnosis", "department", "patient_count", "avg_cost", "avg_stay", "avg_age", "outcome_rate"],
    report_aggregations=["COUNT", "SUM", "AVG"],
    
    primary_key="patient_id",
    date_column="admission_date",
    category_columns=["diagnosis", "department", "gender", "treatment", "insurance", "outcome", "age_group"],
    numeric_columns=["age", "cost", "length_of_stay", "vitals_temp"],
    text_columns=["patient_id", "medication", "lab_results", "vitals_bp"]
)


# ----------------------------------------------------------------------------
# FINANCIAL DATA CONFIGURATION
# ----------------------------------------------------------------------------
DATASET_CONFIGS["financial"] = DatasetConfig(
    name="financial",
    data_type=DataType.FINANCIAL,
    description="Financial transactions and stock market data",
    source_file="data/raw/financial.csv",
    
    columns=[
        ColumnConfig("transaction_id", "string", "Unique transaction ID", is_required=True, is_unique=True),
        ColumnConfig("date", "date", "Transaction date", is_required=True),
        ColumnConfig("type", "string", "Transaction type (buy/sell/transfer)", is_required=True),
        ColumnConfig("symbol", "string", "Stock/asset symbol", is_required=True),
        ColumnConfig("quantity", "float", "Number of units", is_required=True, validation_rules=["positive"]),
        ColumnConfig("price", "float", "Unit price", is_required=True, validation_rules=["positive"]),
        ColumnConfig("total_value", "float", "Total transaction value", is_required=True, validation_rules=["positive"]),
        ColumnConfig("fee", "float", "Transaction fee", is_required=False),
        ColumnConfig("account", "string", "Account identifier", is_required=True),
        ColumnConfig("broker", "string", "Broker name", is_required=False),
        ColumnConfig("sector", "string", "Market sector", is_required=False),
        ColumnConfig("exchange", "string", "Stock exchange", is_required=False),
    ],
    
    cleaning_rules={
        "remove_nulls": ["transaction_id", "symbol", "type"],
        "positive_values": ["quantity", "price", "total_value"],
        "date_format": "YYYY-MM-DD",
        "valid_types": ["buy", "sell", "transfer", "dividend"]
    },
    
    transformation_rules={
        "calculated_columns": {
            "profit_loss": "CASE WHEN type = 'sell' THEN total_value - fee ELSE 0 END",
            "net_value": "total_value - COALESCE(fee, 0)"
        }
    },
    
    aggregation_rules={
        "group_by": ["symbol", "type", "sector"],
        "metrics": ["SUM(total_value)", "SUM(quantity)", "AVG(price)", "COUNT(*)"]
    },
    
    query_patterns={
        "top_holdings": "SELECT symbol, SUM(quantity) as total_qty, SUM(total_value) as total_value FROM {table} WHERE type = 'buy' GROUP BY symbol ORDER BY total_value DESC LIMIT {limit}",
        "by_sector": "SELECT sector, COUNT(*) as transactions, SUM(total_value) as total_value FROM {table} WHERE sector IS NOT NULL GROUP BY sector ORDER BY total_value DESC",
        "daily_volume": "SELECT date, COUNT(*) as transactions, SUM(total_value) as volume FROM {table} GROUP BY date ORDER BY date",
        "profitable_trades": "SELECT symbol, SUM(profit_loss) as total_profit FROM {table} WHERE type = 'sell' GROUP BY symbol ORDER BY total_profit DESC LIMIT {limit}",
        "portfolio_summary": "SELECT symbol, type, SUM(quantity) as qty, AVG(price) as avg_price FROM {table} GROUP BY symbol, type ORDER BY symbol",
        "broker_comparison": "SELECT broker, COUNT(*) as trades, SUM(total_value) as volume, AVG(fee) as avg_fee FROM {table} WHERE broker IS NOT NULL GROUP BY broker ORDER BY volume DESC",
        "monthly_performance": "SELECT strftime(date, '%Y-%m') as month, type, SUM(total_value) as total FROM {table} GROUP BY month, type ORDER BY month",
        "high_value_transactions": "SELECT transaction_id, symbol, type, total_value, date FROM {table} WHERE total_value > 10000 ORDER BY total_value DESC LIMIT {limit}"
    },
    
    report_columns=["symbol", "type", "transaction_count", "total_value", "avg_price", "sector"],
    report_aggregations=["SUM", "COUNT", "AVG"],
    
    primary_key="transaction_id",
    date_column="date",
    category_columns=["type", "symbol", "account", "broker", "sector", "exchange"],
    numeric_columns=["quantity", "price", "total_value", "fee", "profit_loss", "net_value"],
    text_columns=["transaction_id", "symbol", "account"]
)


# ============================================================================
# CONFIGURATION MANAGER
# ============================================================================

class ConfigManager:
    """Manages dataset configurations"""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = config_dir
        self.active_configs: Dict[str, DatasetConfig] = DATASET_CONFIGS.copy()
        self.current_dataset: str = "sales"  # Default
    
    def get_config(self, dataset_name: str) -> Optional[DatasetConfig]:
        """Get configuration for a dataset"""
        return self.active_configs.get(dataset_name)
    
    def get_current_config(self) -> DatasetConfig:
        """Get the currently active dataset configuration"""
        return self.active_configs.get(self.current_dataset)
    
    def set_current_dataset(self, dataset_name: str) -> bool:
        """Set the active dataset"""
        if dataset_name in self.active_configs:
            self.current_dataset = dataset_name
            return True
        return False
    
    def list_datasets(self) -> List[str]:
        """List all available dataset configurations"""
        return list(self.active_configs.keys())
    
    def add_custom_config(self, config: DatasetConfig) -> None:
        """Add a custom dataset configuration"""
        self.active_configs[config.name] = config
    
    def load_from_file(self, filepath: str) -> DatasetConfig:
        """Load configuration from JSON file"""
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        columns = [ColumnConfig(**col) for col in data.get("columns", [])]
        
        config = DatasetConfig(
            name=data["name"],
            data_type=DataType(data.get("data_type", "custom")),
            description=data.get("description", ""),
            source_file=data.get("source_file", ""),
            columns=columns,
            cleaning_rules=data.get("cleaning_rules", {}),
            transformation_rules=data.get("transformation_rules", {}),
            aggregation_rules=data.get("aggregation_rules", {}),
            query_patterns=data.get("query_patterns", {}),
            report_columns=data.get("report_columns", []),
            report_aggregations=data.get("report_aggregations", []),
            primary_key=data.get("primary_key", ""),
            date_column=data.get("date_column", ""),
            category_columns=data.get("category_columns", []),
            numeric_columns=data.get("numeric_columns", []),
            text_columns=data.get("text_columns", [])
        )
        
        self.active_configs[config.name] = config
        return config
    
    def save_to_file(self, dataset_name: str, filepath: str) -> bool:
        """Save configuration to JSON file"""
        config = self.active_configs.get(dataset_name)
        if not config:
            return False
        
        with open(filepath, 'w') as f:
            json.dump(config.to_dict(), f, indent=2)
        
        return True
    
    def auto_detect_schema(self, csv_path: str, dataset_name: str = None) -> DatasetConfig:
        """Auto-detect schema from a CSV file"""
        import pandas as pd
        
        df = pd.read_csv(csv_path)
        
        columns = []
        category_cols = []
        numeric_cols = []
        text_cols = []
        date_col = ""
        
        for col in df.columns:
            dtype = df[col].dtype
            sample = df[col].dropna().head(10).tolist()
            
            # Detect type
            if "date" in col.lower() or "time" in col.lower():
                col_type = "date"
                date_col = col
            elif dtype in ["int64", "int32", "float64", "float32"]:
                col_type = "integer" if "int" in str(dtype) else "float"
                numeric_cols.append(col)
            elif dtype == "object":
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio < 0.1:  # Low cardinality = category
                    col_type = "string"
                    category_cols.append(col)
                else:
                    col_type = "text"
                    text_cols.append(col)
            else:
                col_type = "string"
            
            columns.append(ColumnConfig(
                name=col,
                data_type=col_type,
                description=f"Auto-detected {col_type} column",
                is_required=not df[col].isna().all()
            ))
        
        name = dataset_name or os.path.basename(csv_path).replace(".csv", "")
        
        config = DatasetConfig(
            name=name,
            data_type=DataType.CUSTOM,
            description=f"Auto-detected configuration for {csv_path}",
            source_file=csv_path,
            columns=columns,
            date_column=date_col,
            category_columns=category_cols,
            numeric_columns=numeric_cols,
            text_columns=text_cols
        )
        
        return config


# ============================================================================
# SCHEMA TO SQL CONVERTER
# ============================================================================

class SchemaConverter:
    """Converts schema configurations to SQL"""
    
    @staticmethod
    def get_create_table_sql(config: DatasetConfig, table_name: str = None) -> str:
        """Generate CREATE TABLE SQL from configuration"""
        table = table_name or f"{config.name}_raw"
        
        columns_sql = []
        for col in config.columns:
            sql_type = SchemaConverter._python_type_to_sql(col.data_type)
            constraints = []
            
            if col.is_required:
                constraints.append("NOT NULL")
            if col.is_unique:
                constraints.append("UNIQUE")
            
            col_def = f"    {col.name} {sql_type}"
            if constraints:
                col_def += " " + " ".join(constraints)
            columns_sql.append(col_def)
        
        return f"CREATE TABLE IF NOT EXISTS {table} (\n" + ",\n".join(columns_sql) + "\n)"
    
    @staticmethod
    def _python_type_to_sql(python_type: str) -> str:
        """Convert Python type to SQL type"""
        type_map = {
            "string": "VARCHAR",
            "text": "TEXT",
            "integer": "INTEGER",
            "float": "DOUBLE",
            "date": "DATE",
            "boolean": "BOOLEAN",
            "datetime": "TIMESTAMP"
        }
        return type_map.get(python_type.lower(), "VARCHAR")
    
    @staticmethod
    def get_cleaning_sql(config: DatasetConfig, source_table: str, target_table: str) -> str:
        """Generate data cleaning SQL"""
        cleaning_rules = config.cleaning_rules
        
        # Base select
        select_parts = []
        where_parts = []
        
        for col in config.columns:
            col_select = col.name
            
            # Type casting
            if col.data_type == "date":
                col_select = f"CAST({col.name} AS DATE)"
            elif col.data_type == "integer":
                col_select = f"CAST({col.name} AS INTEGER)"
            elif col.data_type == "float":
                col_select = f"CAST({col.name} AS DOUBLE)"
            
            select_parts.append(col_select)
            
            # Null checks
            if col.is_required and "remove_nulls" in cleaning_rules:
                if col.name in cleaning_rules["remove_nulls"]:
                    where_parts.append(f"{col.name} IS NOT NULL")
        
        # Positive value checks
        if "positive_values" in cleaning_rules:
            for col_name in cleaning_rules["positive_values"]:
                where_parts.append(f"{col_name} > 0")
        
        sql = f"SELECT {', '.join(select_parts)} FROM {source_table}"
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)
        
        return f"CREATE TABLE {target_table} AS {sql}"
    
    @staticmethod
    def get_transformation_sql(config: DatasetConfig, source_table: str, target_table: str) -> str:
        """Generate transformation SQL with calculated columns"""
        trans_rules = config.transformation_rules
        
        select_parts = ["*"]  # Include all original columns
        
        # Add calculated columns
        if "calculated_columns" in trans_rules:
            for col_name, expression in trans_rules["calculated_columns"].items():
                select_parts.append(f"{expression} as {col_name}")
        
        return f"CREATE TABLE {target_table} AS SELECT {', '.join(select_parts)} FROM {source_table}"


# Global config manager instance
config_manager = ConfigManager()
