"""
DataForge AI - Pydantic Output Schemas for LLM Agent
Structured output models for all LLM interactions.
"""

from pydantic import BaseModel, Field
from typing import Dict, List, Optional


# ─── Dataset Analysis Schemas ─────────────────────────────────────────────────


class ColumnAnalysis(BaseModel):
    """Analysis result for a single column in a dataset."""
    semantic_type: str = Field(
        ...,
        description="Semantic type of the column (e.g., money, category, datetime, id, text, score, percentage, count, location, name)",
    )
    business_meaning: str = Field(
        ...,
        description="Human-readable description of what this column represents in business terms",
    )
    data_quality: str = Field(
        ...,
        description="Quality assessment: 'good', 'moderate', or 'poor'",
    )
    issues: List[str] = Field(
        default_factory=list,
        description="List of identified data quality issues for this column",
    )
    recommendations: List[str] = Field(
        default_factory=list,
        description="List of recommended actions to improve this column",
    )


class DataQualitySummary(BaseModel):
    """Overall data quality assessment for a dataset."""
    overall_score: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Overall data quality score from 0.0 (worst) to 1.0 (best)",
    )
    issues: List[str] = Field(
        default_factory=list,
        description="List of data quality issues found across the dataset",
    )
    recommendations: List[str] = Field(
        default_factory=list,
        description="List of recommendations to improve overall data quality",
    )


class RecommendedTransformation(BaseModel):
    """A recommended data transformation operation."""
    type: str = Field(
        ...,
        description="Type of transformation: clean, aggregate, derive, filter, join, pivot, unpivot, or normalize",
    )
    description: str = Field(
        ...,
        description="Human-readable description of the transformation and its purpose",
    )
    sql_template: str = Field(
        ...,
        description="SQL template (DuckDB-compatible) that implements this transformation",
    )


class SuggestedMetric(BaseModel):
    """A suggested business metric or KPI derived from the dataset."""
    name: str = Field(
        ...,
        description="Short name for the metric (e.g., 'Total Revenue', 'Avg Order Value')",
    )
    description: str = Field(
        ...,
        description="Detailed description of what this metric measures",
    )
    formula: str = Field(
        ...,
        description="SQL formula or expression to compute this metric",
    )
    business_value: str = Field(
        ...,
        description="Why this metric is valuable for business decision-making",
    )


class DatasetAnalysis(BaseModel):
    """Complete structured analysis output from the dataset analyzer LLM."""
    dataset_type: str = Field(
        ...,
        description="Primary dataset type (e.g., sales, financial, hr, medical, iot, ecommerce, logistics)",
    )
    dataset_subtype: str = Field(
        default="standard",
        description="More specific subtype (e.g., retail_sales, b2b_financial, patient_records)",
    )
    confidence_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence in the type classification from 0.0 to 1.0",
    )
    column_analysis: Dict[str, ColumnAnalysis] = Field(
        default_factory=dict,
        description="Per-column analysis keyed by column name",
    )
    data_quality_summary: DataQualitySummary = Field(
        default_factory=DataQualitySummary,
        description="Overall data quality assessment",
    )
    recommended_transformations: List[RecommendedTransformation] = Field(
        default_factory=list,
        description="List of recommended data transformations",
    )
    suggested_metrics: List[SuggestedMetric] = Field(
        default_factory=list,
        description="List of suggested business metrics and KPIs",
    )
    natural_language_insights: List[str] = Field(
        default_factory=list,
        description="Natural language insights about the dataset",
    )


# ─── SQL Generation Schema ────────────────────────────────────────────────────


class SQLResult(BaseModel):
    """Structured output from the NL-to-SQL generator LLM."""
    sql: str = Field(
        ...,
        description="The generated DuckDB-compatible SQL query",
    )
    explanation: str = Field(
        ...,
        description="Human-readable explanation of what the SQL query does",
    )
    generated_by: str = Field(
        default="llm",
        description="How this SQL was generated: 'llm' or 'fallback'",
    )


# ─── dbt Model Generation Schemas ─────────────────────────────────────────────


class DBTModel(BaseModel):
    """A single dbt model file."""
    path: str = Field(
        ...,
        description="File path relative to the dbt project root (e.g., models/staging/stg_orders.sql)",
    )
    content: str = Field(
        ...,
        description="Complete SQL content of the dbt model",
    )
    description: str = Field(
        ...,
        description="Human-readable description of what this model does",
    )


class DBTModelsOutput(BaseModel):
    """Structured output from the dbt model generator LLM."""
    models: List[DBTModel] = Field(
        default_factory=list,
        description="List of generated dbt model files",
    )


# ─── Pipeline Generation Schemas ──────────────────────────────────────────────


class PipelineStep(BaseModel):
    """A single step in a data pipeline."""
    name: str = Field(
        ...,
        description="Short identifier for the step (e.g., 'extract', 'clean', 'transform')",
    )
    operation: str = Field(
        ...,
        description="Type of operation performed in this step",
    )
    description: str = Field(
        ...,
        description="Detailed description of what this step does",
    )


class PipelineOutput(BaseModel):
    """Structured output from the pipeline generator LLM."""
    steps: List[PipelineStep] = Field(
        default_factory=list,
        description="Ordered list of pipeline steps",
    )
    description: str = Field(
        ...,
        description="High-level description of the entire pipeline",
    )
    

# ─── Schema Detection Schemas ──────────────────────────────────────────────


class ColumnSemantic(BaseModel):
    """Semantic classification for a single column from LLM."""
    column_name: str = Field(
        ...,
        description="Exact column name from the dataset",
    )
    semantic_type: str = Field(
        ...,
        description="Semantic type: id, name, category, datetime, money, count, score, percentage, location, email, phone, url, text, person, generic",
    )
    business_meaning: str = Field(
        ...,
        description="Short description of what this column represents in business terms",
    )


class SchemaDetectionResult(BaseModel):
    """Structured output from the LLM-powered schema detector."""
    dataset_type: str = Field(
        ...,
        description="Dataset type: sales, education, medical, finance, hr, news, ecommerce, iot, logistics, generic",
    )
    dataset_description: str = Field(
        ...,
        description="One-sentence description of what this dataset contains",
    )
    confidence_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence in classification from 0.0 to 1.0",
    )
    column_semantics: List[ColumnSemantic] = Field(
        default_factory=list,
        description="Semantic classification for each column",
    )
    suggested_queries: List[str] = Field(
        default_factory=list,
        description="5 relevant suggested queries for exploring this dataset",
    )