"use client";

import { useState, useEffect } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { LogOut } from "lucide-react";
import { 
  Zap, 
  Bot, 
  GitBranch, 
  Database, 
  FileText, 
  FolderOpen,
  BarChart3,
  Home,
  Menu,
  X,
  Moon,
  Sun,
  CheckCircle2,
  Loader2,
  Sparkles,
  ArrowRight,
  Play,
  Shield,
  Cpu,
  Workflow,
  Upload,
  Table,
  Layers,
  Info,
  Cloud,
  FileSpreadsheet,
  Code,
  RefreshCw,
  Download,
  Search,
  Eye,
  FileCode

} from "lucide-react";
import { useTheme } from "next-themes";
import { AuthDialog } from "@/components/auth-dialog";
import { useAuthStore } from "@/lib/auth-store";
import { CommandBox } from "@/components/dataforge/command-box";
import { FileCard, FileCardSkeleton } from "@/components/dataforge/file-card";
import { QueryBox } from "@/components/dataforge/query-box";
import { FileExplorer } from "@/components/dataforge/file-explorer";
import { PipelineDAG } from "@/components/dataforge/pipeline-dag";
import { StatsCards } from "@/components/dataforge/stats-cards";

// Types
interface ExecutionResult {
  status: string;
  command?: string;
  files: string[];
  logs: string[];
  duration?: number;
  message?: string;
}

interface FileItem {
  name: string;
  path: string;
  type: string;
  category: string;
  size: number;
  content?: string;
  modified?: string;
}

interface QueryResult {
  sql: string;
  columns: string[];
  data: Record<string, unknown>[];
  row_count: number;
  execution_time?: number;
  schema_info?: SchemaInfo;
  explanation?: string;
  generated_by?: string;
}

interface SchemaInfo {
  table: string;
  dataset_type: string;
  columns: Record<string, ColumnInfo>;
  suggested_queries?: string[];
}

interface ColumnInfo {
  type: string;
  semantic: string;
  sample_values?: unknown[];
  unique_count?: number;
}

interface DashboardStats {
  total_pipelines: number;
  total_executions: number;
  success_rate: number;
  tables: number;
  reports: number;
  data_volume: number;
  dataset_type?: string;
  current_table?: string;
  features?: {
    llm_enabled: boolean;
    xlsx_support: boolean;
    airbyte_connected: boolean;
  };
}

interface ChartData {
  pipeline_runs: Array<{ date: string; runs: number; success: number }>;
  primary_chart: Array<{ category: string; value: number }>;
  secondary_chart: Array<{ category: string; value: number }>;
  trend_chart: Array<{ period: string; value: number }>;
}

interface LLMAnalysis {
  dataset_type: string;
  dataset_subtype?: string;
  confidence_score?: number;
  column_analysis?: Record<string, {
    semantic_type: string;
    business_meaning: string;
    data_quality: string;
    issues: string[];
    recommendations: string[];
  }>;
  data_quality_summary?: {
    overall_score: number;
    issues: string[];
    recommendations: string[];
  };
  recommended_transformations?: Array<{
    type: string;
    description: string;
    sql_template: string;
  }>;
  suggested_metrics?: Array<{
    name: string;
    description: string;
    formula: string;
    business_value: string;
  }>;
  visualization_recommendations?: Array<{
    type: string;
    columns: string[];
    purpose: string;
  }>;
  natural_language_insights?: string[];
}

interface AirbyteSource {
  sourceId: string;
  name: string;
  sourceDefinitionId: string;
  connectionConfiguration?: Record<string, unknown>;
}

interface AirbyteConnection {
  connectionId: string;
  name: string;
  sourceId: string;
  destinationId: string;
  status: string;
}

interface AirbyteSourceDefinition {
  sourceDefinitionId: string;
  name: string;
  sourceType?: string;
}

// Schema Display Component
function SchemaDisplay({ schema }: { schema: SchemaInfo | null }) {
  if (!schema) {
    return (
      <Card className="border-dashed">
        <CardContent className="flex flex-col items-center justify-center py-8">
          <Upload className="h-12 w-12 text-muted-foreground mb-4" />
          <p className="text-muted-foreground text-center">
            No dataset loaded. Upload a CSV, JSON, or XLSX file to get started.
          </p>
        </CardContent>
      </Card>
    );
  }

  const datasetTypeColors: Record<string, string> = {
    sales: "bg-green-500/10 text-green-600",
    news: "bg-blue-500/10 text-blue-600",
    medical: "bg-red-500/10 text-red-600",
    finance: "bg-yellow-500/10 text-yellow-600",
    hr: "bg-purple-500/10 text-purple-600",
    iot: "bg-cyan-500/10 text-cyan-600",
    generic: "bg-gray-500/10 text-gray-600"
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <Badge className={datasetTypeColors[schema.dataset_type] || datasetTypeColors.generic}>
          {schema.dataset_type.toUpperCase()}
        </Badge>
        <span className="text-sm text-muted-foreground">
          Table: {schema.table}
        </span>
      </div>

      <div className="grid gap-2 md:grid-cols-2 lg:grid-cols-3">
        {Object.entries(schema.columns).map(([colName, colInfo]) => (
          <Card key={colName} className="p-3">
            <div className="flex items-start justify-between">
              <div>
                <p className="font-medium text-sm">{colName}</p>
                <p className="text-xs text-muted-foreground">{colInfo.semantic}</p>
              </div>
              <Badge variant="outline" className="text-xs">
                {colInfo.type}
              </Badge>
            </div>
            {colInfo.sample_values && colInfo.sample_values.length > 0 && (
              <p className="text-xs text-muted-foreground mt-2 truncate">
                Example: {String(colInfo.sample_values[0])}
              </p>
            )}
          </Card>
        ))}
      </div>

      {schema.suggested_queries && schema.suggested_queries.length > 0 && (
        <Card className="p-4">
          <div className="flex items-center gap-2 mb-3">
            <Sparkles className="h-4 w-4 text-violet-500" />
            <p className="text-sm font-medium">Suggested Queries</p>
          </div>
          <div className="flex flex-wrap gap-2">
            {schema.suggested_queries.map((query, i) => (
              <Badge key={i} variant="secondary" className="cursor-pointer hover:bg-secondary/80">
                {query}
              </Badge>
            ))}
          </div>
        </Card>
      )}
    </div>
  );
}

// LLM Analysis Display Component
function LLMAnalysisDisplay({ analysis, isLoading }: { analysis: LLMAnalysis | null; isLoading: boolean }) {
  if (isLoading) {
    return (
      <Card>
        <CardContent className="flex items-center justify-center py-8">
          <Loader2 className="h-8 w-8 animate-spin text-violet-500 mr-2" />
          <span>AI is analyzing your dataset...</span>
        </CardContent>
      </Card>
    );
  }

  if (!analysis) {
    return (
      <Card className="border-dashed">
        <CardContent className="flex flex-col items-center justify-center py-8">
          <Bot className="h-12 w-12 text-muted-foreground mb-4" />
          <p className="text-muted-foreground text-center mb-4">
            Click &quot;Analyze with AI&quot; to get intelligent insights about your dataset
          </p>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <Card className="p-4">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm text-muted-foreground">Detected Type</p>
            <p className="text-xl font-bold capitalize">{analysis.dataset_type}</p>
          </div>
          {analysis.confidence_score && (
            <div className="text-right">
              <p className="text-sm text-muted-foreground">Confidence</p>
              <p className="text-xl font-bold">{Math.round(analysis.confidence_score * 100)}%</p>
            </div>
          )}
        </div>
      </Card>

      {analysis.natural_language_insights && analysis.natural_language_insights.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <Sparkles className="h-4 w-4 text-violet-500" />
              AI Insights
            </CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2">
              {analysis.natural_language_insights.map((insight, i) => (
                <li key={i} className="flex items-start gap-2 text-sm">
                  <CheckCircle2 className="h-4 w-4 text-green-500 mt-0.5 shrink-0" />
                  {insight}
                </li>
              ))}
            </ul>
          </CardContent>
        </Card>
      )}

      {analysis.recommended_transformations && analysis.recommended_transformations.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <Workflow className="h-4 w-4 text-blue-500" />
              Recommended Transformations
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {analysis.recommended_transformations.map((transform, i) => (
                <div key={i} className="border rounded-lg p-3">
                  <div className="flex items-center gap-2 mb-2">
                    <Badge variant="outline">{transform.type}</Badge>
                    <span className="text-sm font-medium">{transform.description}</span>
                  </div>
                  <pre className="text-xs bg-muted p-2 rounded overflow-x-auto">
                    {transform.sql_template}
                  </pre>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {analysis.suggested_metrics && analysis.suggested_metrics.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <BarChart3 className="h-4 w-4 text-green-500" />
              Suggested Metrics
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid gap-2 md:grid-cols-2">
              {analysis.suggested_metrics.map((metric, i) => (
                <div key={i} className="border rounded-lg p-3">
                  <p className="font-medium text-sm">{metric.name}</p>
                  <p className="text-xs text-muted-foreground">{metric.description}</p>
                  <p className="text-xs mt-1"><strong>Formula:</strong> {metric.formula}</p>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {analysis.column_analysis && Object.keys(analysis.column_analysis).length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <Table className="h-4 w-4 text-blue-500" />
              Column Quality
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto scroll-smooth pb-2">
              <table className="w-full min-w-[900px] text-sm">
                <thead>
                  <tr className="border-b text-left">
                    <th className="py-2 pr-4">Column</th>
                    <th className="py-2 pr-4">Semantic</th>
                    <th className="py-2 pr-4">Quality</th>
                    <th className="py-2 pr-4">Key Issues</th>
                    <th className="py-2 pr-4">Recommendations</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(analysis.column_analysis).map(([colName, col]) => (
                    <tr key={colName} className="border-b align-top">
                      <td className="py-2 pr-4 font-medium whitespace-nowrap">{colName}</td>
                      <td className="py-2 pr-4 whitespace-nowrap">{col.semantic_type}</td>
                      <td className="py-2 pr-4 whitespace-nowrap">{col.data_quality}</td>
                      <td className="py-2 pr-4">{(col.issues || []).slice(0, 2).join(" | ") || "-"}</td>
                      <td className="py-2 pr-4">{(col.recommendations || []).slice(0, 2).join(" | ") || "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

      {analysis.data_quality_summary && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <Shield className="h-4 w-4 text-orange-500" />
              Data Quality Score: {Math.round(analysis.data_quality_summary.overall_score * 100)}%
            </CardTitle>
          </CardHeader>
          <CardContent>
            {analysis.data_quality_summary.issues.length > 0 && (
              <div className="mb-3">
                <p className="text-sm font-medium mb-1">Issues:</p>
                <ul className="text-xs text-muted-foreground space-y-1">
                  {analysis.data_quality_summary.issues.map((issue, i) => (
                    <li key={i}>• {issue}</li>
                  ))}
                </ul>
              </div>
            )}
            {analysis.data_quality_summary.recommendations.length > 0 && (
              <div>
                <p className="text-sm font-medium mb-1">Recommendations:</p>
                <ul className="text-xs text-muted-foreground space-y-1">
                  {analysis.data_quality_summary.recommendations.map((rec, i) => (
                    <li key={i}>• {rec}</li>
                  ))}
                </ul>
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}

// Airbyte Connector Manager Component
function AirbyteConnectorManager() {
  const [sourceDefinitions, setSourceDefinitions] = useState<AirbyteSourceDefinition[]>([]);
  const [sources, setSources] = useState<AirbyteSource[]>([]);
  const [connections, setConnections] = useState<AirbyteConnection[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [selectedSourceType, setSelectedSourceType] = useState<string>("");
  const [connectionConfig, setConnectionConfig] = useState<Record<string, string>>({});
  const [sourceName, setSourceName] = useState("");

  useEffect(() => {
    fetchSourceDefinitions();
    fetchSources();
    fetchConnections();
  }, []);

  const fetchSourceDefinitions = async () => {
    try {
      const response = await fetch("/api/airbyte/source-definitions");
      const data = await response.json();
      setSourceDefinitions(data.source_definitions || []);
    } catch (error) {
      console.error("Failed to fetch source definitions:", error);
    }
  };

  const fetchSources = async () => {
    try {
      const response = await fetch("/api/airbyte/sources");
      const data = await response.json();
      setSources(data.sources || []);
    } catch (error) {
      console.error("Failed to fetch sources:", error);
    }
  };

  const fetchConnections = async () => {
    try {
      const response = await fetch("/api/airbyte/connections");
      const data = await response.json();
      setConnections(data.connections || []);
    } catch (error) {
      console.error("Failed to fetch connections:", error);
    }
  };

  const fetchTemplate = async (sourceType: string) => {
    try {
      const response = await fetch(`/api/airbyte/templates/${sourceType}`);
      const data = await response.json();
      setConnectionConfig(data.template || {});
    } catch (error) {
      console.error("Failed to fetch template:", error);
    }
  };

  const handleSourceTypeChange = (value: string) => {
    setSelectedSourceType(value);
    fetchTemplate(value);
  };

  const handleCreateSource = async () => {
    if (!sourceName || !selectedSourceType) return;
    
    setIsLoading(true);
    try {
      const response = await fetch("/api/airbyte/sources", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: sourceName,
          source_type: selectedSourceType,
          connection_config: connectionConfig
        })
      });
      
      if (response.ok) {
        fetchSources();
        setSourceName("");
        setConnectionConfig({});
      }
    } catch (error) {
      console.error("Failed to create source:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSync = async (connectionId: string) => {
    try {
      await fetch(`/api/airbyte/connections/${connectionId}/sync`, {
        method: "POST"
      });
    } catch (error) {
      console.error("Failed to trigger sync:", error);
    }
  };

  const popularConnectors = [
    { id: "postgres", name: "PostgreSQL", icon: "🐘" },
    { id: "mysql", name: "MySQL", icon: "🐬" },
    { id: "mongodb", name: "MongoDB", icon: "🍃" },
    { id: "s3", name: "Amazon S3", icon: "🪣" },
    { id: "bigquery", name: "BigQuery", icon: "📊" },
    { id: "salesforce", name: "Salesforce", icon: "☁️" },
    { id: "stripe", name: "Stripe", icon: "💳" },
    { id: "google_sheets", name: "Google Sheets", icon: "📋" },
  ];

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <Cloud className="h-5 w-5 text-blue-500" />
            Connect Data Sources
          </CardTitle>
          <CardDescription>
            Connect to 300+ data sources via Airbyte
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid gap-3 md:grid-cols-4">
            {popularConnectors.map((connector) => (
              <Card 
                key={connector.id}
                className="cursor-pointer hover:border-primary/50 transition-colors p-4"
                onClick={() => {
                  setSelectedSourceType(connector.id);
                  fetchTemplate(connector.id);
                }}
              >
                <div className="flex items-center gap-3">
                  <span className="text-2xl">{connector.icon}</span>
                  <span className="font-medium">{connector.name}</span>
                </div>
              </Card>
            ))}
          </div>
        </CardContent>
      </Card>

      {selectedSourceType && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Configure {selectedSourceType.toUpperCase()} Source</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <Label htmlFor="sourceName">Source Name</Label>
              <Input
                id="sourceName"
                value={sourceName}
                onChange={(e) => setSourceName(e.target.value)}
                placeholder="My Data Source"
              />
            </div>
            
            {Object.keys(connectionConfig).length > 0 && (
              <div className="space-y-3">
                <Label>Connection Details</Label>
                {Object.entries(connectionConfig).map(([key, value]) => (
                  <div key={key}>
                    <Label htmlFor={key} className="text-xs">{key}</Label>
                    <Input
                      id={key}
                      value={connectionConfig[key] || ""}
                      onChange={(e) => setConnectionConfig({
                        ...connectionConfig,
                        [key]: e.target.value
                      })}
                      placeholder={String(value)}
                      type={key.includes("password") || key.includes("secret") ? "password" : "text"}
                    />
                  </div>
                ))}
              </div>
            )}
            
            <Button onClick={handleCreateSource} disabled={isLoading}>
              {isLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : null}
              Create Source
            </Button>
          </CardContent>
        </Card>
      )}

      {sources.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Configured Sources</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {sources.map((source) => (
                <div key={source.sourceId} className="flex items-center justify-between border rounded-lg p-3">
                  <div>
                    <p className="font-medium">{source.name}</p>
                    <p className="text-xs text-muted-foreground">{source.sourceDefinitionId}</p>
                  </div>
                  <div className="flex gap-2">
                    <Button size="sm" variant="outline">Test</Button>
                    <Button size="sm" variant="destructive">Delete</Button>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {connections.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Active Connections</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {connections.map((conn) => (
                <div key={conn.connectionId} className="flex items-center justify-between border rounded-lg p-3">
                  <div>
                    <p className="font-medium">{conn.name}</p>
                    <Badge variant={conn.status === "active" ? "default" : "secondary"}>
                      {conn.status}
                    </Badge>
                  </div>
                  <Button size="sm" onClick={() => handleSync(conn.connectionId)}>
                    <RefreshCw className="h-4 w-4 mr-1" />
                    Sync
                  </Button>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

// DBT Models Display Component
function DBTModelsDisplay({ models, isLoading }: { models: Array<{ path: string; content: string; description: string }> | null; isLoading: boolean }) {
  const [selectedModel, setSelectedModel] = useState<string | null>(null);

  if (isLoading) {
    return (
      <Card>
        <CardContent className="flex items-center justify-center py-8">
          <Loader2 className="h-8 w-8 animate-spin text-violet-500 mr-2" />
          <span>AI is generating dbt models...</span>
        </CardContent>
      </Card>
    );
  }

  if (!models || models.length === 0) {
    return (
      <Card className="border-dashed">
        <CardContent className="flex flex-col items-center justify-center py-8">
          <Code className="h-12 w-12 text-muted-foreground mb-4" />
          <p className="text-muted-foreground text-center mb-4">
            Click &quot;Generate dbt Models&quot; to create transformation models
          </p>
        </CardContent>
      </Card>
    );
  }

  const currentModel = models.find(m => m.path === selectedModel) || models[0];

  return (
    <div className="grid gap-4 md:grid-cols-3">
      <Card className="md:col-span-1">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Generated Models</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {models.map((model) => (
              <div
                key={model.path}
                className={`p-2 rounded cursor-pointer border ${
                  selectedModel === model.path ? "border-primary bg-primary/5" : "hover:border-muted"
                }`}
                onClick={() => setSelectedModel(model.path)}
              >
                <p className="text-sm font-medium truncate">{model.path}</p>
                <p className="text-xs text-muted-foreground truncate">{model.description}</p>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <Card className="md:col-span-2">
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base">{currentModel?.path}</CardTitle>
            <Button size="sm" variant="outline">
              <Download className="h-4 w-4 mr-1" />
              Download
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <pre className="text-xs bg-muted p-4 rounded-lg overflow-x-auto max-h-96">
            {currentModel?.content}
          </pre>
        </CardContent>
      </Card>
    </div>
  );
}

// File Upload Component (Enhanced with XLSX support)
function FileUploader({ onUploadComplete }: { onUploadComplete: (result?: any) => void }) {
  const [uploading, setUploading] = useState(false);
  const [dragActive, setDragActive] = useState(false);

  const handleUpload = async (file: File) => {
    const ext = file.name.split('.').pop()?.toLowerCase();
    if (!['csv', 'json', 'xlsx', 'xls', 'xlsm'].includes(ext || '')) {
      alert('Please upload a CSV, JSON, or Excel file');
      return;
    }

    setUploading(true);
    const formData = new FormData();
    formData.append('file', file);

    try {
      const response = await fetch('/api/upload', {
        method: 'POST',
        body: formData
      });
      
      const result = await response.json();
      
      if (result.status === 'success') {
        onUploadComplete(result);
      }
    } catch (error) {
      console.error('Upload failed:', error);
    } finally {
      setUploading(false);
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleUpload(e.dataTransfer.files[0]);
    }
  };

  return (
    <Card 
      className={`border-dashed ${dragActive ? 'border-primary bg-primary/5' : ''}`}
      onDragEnter={() => setDragActive(true)}
      onDragLeave={() => setDragActive(false)}
      onDragOver={(e) => e.preventDefault()}
      onDrop={handleDrop}
    >
      <CardContent className="flex flex-col items-center justify-center py-8">
        {uploading ? (
          <>
            <Loader2 className="h-12 w-12 text-muted-foreground animate-spin mb-4" />
            <p className="text-muted-foreground">Uploading and processing...</p>
          </>
        ) : (
          <>
            <Upload className="h-12 w-12 text-muted-foreground mb-4" />
            <p className="text-muted-foreground text-center mb-2">
              Drag & drop your <strong>CSV, JSON, or Excel</strong> file here
            </p>
            <p className="text-sm text-muted-foreground mb-4">
              Supports .csv, .json, .xlsx, .xls files
            </p>
            <label>
              <input
                type="file"
                accept=".csv,.json,.xlsx,.xls,.xlsm"
                className="hidden"
                onChange={(e) => e.target.files?.[0] && handleUpload(e.target.files[0])}
              />
              <Button variant="outline" asChild>
                <span>Browse Files</span>
              </Button>
            </label>
          </>
        )}
      </CardContent>
    </Card>
  );
}

// ✅ FIX: Active File Bar (like ChatGPT attachment bar)
function ActiveFileBar({ file, onClear, onPreview }: {
  file: FileItem;
  onClear: () => void;
  onPreview: () => void;
}) {
  return (
    <div className="flex items-center gap-3 p-3 bg-muted/50 rounded-lg border">
      <FileSpreadsheet className="h-4 w-4 text-green-500 shrink-0" />
      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium truncate">{file.name}</p>
        <p className="text-xs text-muted-foreground truncate">{file.path}</p>
      </div>
      <Badge variant="secondary" className="text-xs shrink-0">
        {file.category.replace('_', ' ')}
      </Badge>
      <Badge variant="outline" className="text-xs shrink-0">
        {file.size < 1024 ? `${file.size} B` : `${(file.size / 1024).toFixed(1)} KB`}
      </Badge>
      <Button variant="ghost" size="icon" className="h-7 w-7 shrink-0" onClick={onPreview} title="Preview file">
        <Eye className="h-3.5 w-3.5" />
      </Button>
      <Button variant="ghost" size="icon" className="h-7 w-7 shrink-0 text-muted-foreground hover:text-red-500" onClick={onClear} title="Remove file">
        <X className="h-3.5 w-3.5" />
      </Button>
    </div>
  );
}

// Sample Datasets Component
function SampleDatasets({ onSelect }: { onSelect: (filename: string) => void }) {
  const samples = [
    { name: 'news_articles.csv', type: 'News', icon: '📰', description: '25 news articles with engagement metrics' },
    { name: 'medical_records.csv', type: 'Medical', icon: '🏥', description: '25 patient records with treatments' },
    { name: 'finance_stocks.csv', type: 'Finance', icon: '📈', description: '30 stock records with market data' },
    { name: 'sales.csv', type: 'Sales', icon: '🛒', description: 'Sample sales transaction data' },
  ];

  const loadSample = async (filename: string) => {
    try {
      const response = await fetch(`/api/load-sample?file=${filename}`, {
        method: 'POST'
      });
      const result = await response.json();
      if (result.status === 'success') {
        onSelect(filename);
      }
    } catch (error) {
      console.error('Failed to load sample:', error);
    }
  };

  return (
    <div className="grid gap-3 md:grid-cols-2">
      {samples.map((sample) => (
        <Card 
          key={sample.name} 
          className="cursor-pointer hover:border-primary/50 transition-colors"
          onClick={() => loadSample(sample.name)}
        >
          <CardContent className="p-4 flex items-center gap-3">
            <span className="text-2xl">{sample.icon}</span>
            <div className="flex-1">
              <p className="font-medium">{sample.name}</p>
              <p className="text-sm text-muted-foreground">{sample.description}</p>
            </div>
            <Badge variant="outline">{sample.type}</Badge>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

// Warehouse View Component
function WarehouseView() {
    const [warehouseInfo, setWarehouseInfo] = useState<{
    tables: Array<{ name: string; row_count: number; columns: Array<{ name: string; type: string }> }>;
    count: number;
    data_volume?: number;
  } | null>(null);
  const [schemaCache, setSchemaCache] = useState<SchemaInfo | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    fetchWarehouseInfo();
    fetchSchemaCache();
  }, []);

  const fetchWarehouseInfo = async () => {
    setIsLoading(true);
    try {
      const response = await fetch("/api/warehouse");
      const data = await response.json();
      setWarehouseInfo(data);
    } catch (error) {
      console.error("Failed to fetch warehouse info:", error);
      setWarehouseInfo({ tables: [], count: 0, data_volume: 0 });
    } finally {
      setIsLoading(false);
    }
  };

  const fetchSchemaCache = async () => {
    try {
      const response = await fetch("/api/schema");
      const data = await response.json();
      if (data.schema && data.schema.columns && Object.keys(data.schema.columns).length > 0) {
        setSchemaCache(data.schema);
      }
    } catch (error) {
      console.error("Failed to fetch schema cache:", error);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2">
        <Database className="h-6 w-6 text-green-500" />
        <h1 className="text-2xl font-bold">Data Warehouse</h1>
      </div>
      <p className="text-muted-foreground">
        Built-in DuckDB warehouse for fast analytics and data storage.
      </p>

      <div className="grid gap-4 md:grid-cols-3">
        <Card className="hover:shadow-md transition-shadow">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Tables</CardTitle>
            <div className="p-2 rounded-lg bg-green-500/10">
              <Database className="h-4 w-4 text-green-500" />
            </div>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {isLoading ? (
                <Loader2 className="h-6 w-6 animate-spin" />
              ) : (
                warehouseInfo?.count ?? 0
              )}
            </div>
            <p className="text-xs text-muted-foreground mt-1">Active tables in warehouse</p>
          </CardContent>
        </Card>

        <Card className="hover:shadow-md transition-shadow">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Data Volume</CardTitle>
            <div className="p-2 rounded-lg bg-blue-500/10">
              <Layers className="h-4 w-4 text-blue-500" />
            </div>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {warehouseInfo?.data_volume ? `${(warehouseInfo.data_volume / (1024 * 1024)).toFixed(1)} MB` : "—"}
            </div>
            <p className="text-xs text-muted-foreground mt-1">Total data stored</p>
          </CardContent>
        </Card>

        <Card className="hover:shadow-md transition-shadow">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Schema Cache</CardTitle>
            <div className="p-2 rounded-lg bg-violet-500/10">
              <Cpu className="h-4 w-4 text-violet-500" />
            </div>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {schemaCache ? schemaCache.dataset_type.toUpperCase() : "None"}
            </div>
            <p className="text-xs text-muted-foreground mt-1">
              {schemaCache ? `${Object.keys(schemaCache.columns).length} columns cached` : "Upload data to cache schema"}
            </p>
          </CardContent>
        </Card>
      </div>

      <Card className="shadow-lg">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="flex items-center gap-2">
              <Table className="h-5 w-5 text-green-500" />
              Warehouse Tables
            </CardTitle>
            <Button variant="outline" size="sm" onClick={fetchWarehouseInfo} disabled={isLoading}>
              {isLoading ? <Loader2 className="h-4 w-4 animate-spin mr-1" /> : <RefreshCw className="h-4 w-4 mr-1" />}
              Refresh
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground mr-2" />
              <span className="text-muted-foreground">Loading warehouse data...</span>
            </div>
          ) : warehouseInfo && warehouseInfo.tables && warehouseInfo.tables.length > 0 ? (
            <div className="space-y-2">
                            {warehouseInfo.tables.map((table, i) => (
                <div key={i} className="flex items-center gap-3 border rounded-lg p-3">
                  <Database className="h-4 w-4 text-green-500" />
                  <span className="font-medium">{table.name}</span>
                  <span className="text-xs text-muted-foreground">{table.row_count} rows &middot; {table.columns.length} columns</span>
                  <Badge variant="outline" className="ml-auto">active</Badge>
                </div>
              ))}
            </div>
          ) : (
            <div className="flex flex-col items-center justify-center py-8">
              <Database className="h-12 w-12 text-muted-foreground mb-4" />
              <p className="text-muted-foreground text-center">
                No tables in warehouse yet. Upload data to get started.
              </p>
              <Button variant="outline" className="mt-4" onClick={fetchWarehouseInfo}>
                <RefreshCw className="h-4 w-4 mr-1" />
                Check Again
              </Button>
            </div>
          )}
        </CardContent>
      </Card>

      {schemaCache && (
        <Card className="shadow-lg">
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2">
              <Cpu className="h-5 w-5 text-violet-500" />
              Schema Cache
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-2 mb-4">
              <Badge>{schemaCache.dataset_type.toUpperCase()}</Badge>
              <span className="text-sm text-muted-foreground">Table: {schemaCache.table}</span>
              <span className="text-sm text-muted-foreground">• {Object.keys(schemaCache.columns).length} columns</span>
            </div>
            <div className="grid gap-2 md:grid-cols-2 lg:grid-cols-3">
              {Object.entries(schemaCache.columns).map(([colName, colInfo]) => (
                <div key={colName} className="border rounded-lg p-2">
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium">{colName}</span>
                    <Badge variant="outline" className="text-xs">{colInfo.type}</Badge>
                  </div>
                  <span className="text-xs text-muted-foreground">{colInfo.semantic}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

// Landing Page Component
function LandingPage({ onGetStarted }: { onGetStarted: () => void }) {
  const { setTheme, resolvedTheme } = useTheme();
  const [mounted, setMounted] = useState(false);
  useEffect(() => { setMounted(true); }, []);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [authDialogOpen, setAuthDialogOpen] = useState(false);

  const features = [
    { icon: Bot, title: "AI-Powered Agent", description: "LLM-powered intelligent analysis, dbt generation, and NL→SQL conversion", color: "text-violet-500", bgColor: "bg-violet-500/10" },
    { icon: GitBranch, title: "Automated Pipelines", description: "Generate, execute, and monitor ETL pipelines with intelligent orchestration", color: "text-blue-500", bgColor: "bg-blue-500/10" },
    { icon: Database, title: "Data Warehouse", description: "Built-in DuckDB warehouse for fast analytics and data storage", color: "text-green-500", bgColor: "bg-green-500/10" },
    { icon: Cloud, title: "Airbyte Integration", description: "Connect to 300+ data sources with real Airbyte integration", color: "text-cyan-500", bgColor: "bg-cyan-500/10" },
    { icon: FileSpreadsheet, title: "Excel Support", description: "Upload and process XLSX files with multi-sheet support", color: "text-emerald-500", bgColor: "bg-emerald-500/10" },
    { icon: Code, title: "dbt Generation", description: "AI generates human-quality dbt transformation models", color: "text-orange-500", bgColor: "bg-orange-500/10" }
  ];

  const steps = [
    { step: 1, title: "Upload Data", description: "Upload any CSV, JSON, or Excel file" },
    { step: 2, title: "AI Analysis", description: "LLM detects schema and type" },
    { step: 3, title: "Generate Pipeline", description: "AI creates dbt models and transforms" },
    { step: 4, title: "Query & Visualize", description: "Natural language queries" }
  ];

  return (
    <div className="min-h-screen bg-background">
      <nav className="fixed top-0 left-0 right-0 z-50 bg-background/80 backdrop-blur-md border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center h-16">
            <div className="flex items-center gap-2">
              <div className="w-8 h-8 bg-gradient-to-br from-violet-600 to-cyan-500 rounded-lg flex items-center justify-center">
                <Zap className="w-5 h-5 text-white" />
              </div>
              <span className="font-bold text-xl">DataForge AI</span>
              <Badge variant="secondary" className="ml-2">v3.0</Badge>
            </div>
            <div className="hidden md:flex items-center gap-8">
              <a href="#features" className="text-muted-foreground hover:text-foreground transition-colors">Features</a>
              <a href="#how-it-works" className="text-muted-foreground hover:text-foreground transition-colors">How It Works</a>
              <a href="#pricing" className="text-muted-foreground hover:text-foreground transition-colors">Pricing</a>
            </div>
            <div className="flex items-center gap-2">
              <Button variant="ghost" size="icon" disabled={!mounted} onClick={() => setTheme(resolvedTheme === "dark" ? "light" : "dark")}>
                {mounted && resolvedTheme === "dark" ? <Sun className="h-5 w-5" /> : <Moon className="h-5 w-5" />}
              </Button>
              <Button onClick={() => setAuthDialogOpen(true)} className="bg-gradient-to-r from-violet-600 to-cyan-500 hover:from-violet-700 hover:to-cyan-600">
                Get Started
              </Button>
              <Button variant="ghost" size="icon" className="md:hidden" onClick={() => setMobileMenuOpen(!mobileMenuOpen)}>
                {mobileMenuOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
              </Button>
            </div>
          </div>
        </div>
        {mobileMenuOpen && (
          <div className="md:hidden border-t bg-background p-4 space-y-4">
            <a href="#features" className="block text-muted-foreground">Features</a>
            <a href="#how-it-works" className="block text-muted-foreground">How It Works</a>
            <a href="#pricing" className="block text-muted-foreground">Pricing</a>
            <Separator />
            <Button className="w-full bg-gradient-to-r from-violet-600 to-cyan-500" onClick={() => setAuthDialogOpen(true)}>Get Started</Button>
          </div>
        )}
      </nav>

      <section className="pt-32 pb-20 px-4 sm:px-6 lg:px-8">
        <div className="max-w-7xl mx-auto">
          <div className="text-center max-w-4xl mx-auto">
            <div className="flex items-center justify-center gap-2 mb-4">
              <Badge variant="secondary" className="px-4 py-1"><Sparkles className="h-3 w-3 mr-1" />LLM-Powered</Badge>
              <Badge variant="secondary" className="px-4 py-1"><FileSpreadsheet className="h-3 w-3 mr-1" />XLSX Support</Badge>
              <Badge variant="secondary" className="px-4 py-1"><Cloud className="h-3 w-3 mr-1" />Airbyte Ready</Badge>
            </div>
            <h1 className="text-4xl sm:text-5xl lg:text-6xl font-bold tracking-tight mb-6">
              Build Data Pipelines with{" "}
              <span className="bg-gradient-to-r from-violet-600 to-cyan-500 bg-clip-text text-transparent">AI</span>{" "}
              — No Coding Required
            </h1>
            <p className="text-lg sm:text-xl text-muted-foreground mb-8 max-w-2xl mx-auto">
              Upload <strong>any dataset</strong> - sales, news, medical, finance, or custom data. 
              AI automatically detects schema, generates dbt models, and creates production-ready pipelines.
            </p>
            <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
              <Button size="lg" onClick={() => setAuthDialogOpen(true)} className="bg-gradient-to-r from-violet-600 to-cyan-500 hover:from-violet-700 hover:to-cyan-600 h-12 px-8">
                Get Started Free <ArrowRight className="ml-2 h-4 w-4" />
              </Button>
              <Button size="lg" variant="outline" className="h-12 px-8">
                <Play className="mr-2 h-4 w-4" />Watch Demo
              </Button>
            </div>
          </div>
          <div className="mt-16 relative">
            <div className="absolute inset-0 bg-gradient-to-r from-violet-600/20 to-cyan-500/20 blur-3xl rounded-3xl" />
            <Card className="relative bg-background/50 backdrop-blur border-2 shadow-2xl">
              <CardContent className="p-6">
                <div className="flex items-center gap-4 mb-4">
                  <div className="w-10 h-10 bg-gradient-to-br from-violet-600 to-cyan-500 rounded-lg flex items-center justify-center">
                    <Bot className="w-6 h-6 text-white" />
                  </div>
                  <div>
                    <p className="font-semibold">DataForge AI Agent</p>
                    <p className="text-sm text-muted-foreground">LLM-powered analysis</p>
                  </div>
                </div>
                <div className="bg-muted rounded-lg p-4 mb-4">
                  <p className="text-muted-foreground mb-2">User command:</p>
                  <p className="font-medium">&quot;Analyze this medical dataset and generate dbt models&quot;</p>
                </div>
                <div className="space-y-2">
                  <div className="flex items-center gap-2"><CheckCircle2 className="h-5 w-5 text-green-500" /><span className="text-sm">Detected: MEDICAL dataset (98% confidence)</span></div>
                  <div className="flex items-center gap-2"><CheckCircle2 className="h-5 w-5 text-green-500" /><span className="text-sm">Generated: 3 dbt models (staging, intermediate, mart)</span></div>
                  <div className="flex items-center gap-2"><CheckCircle2 className="h-5 w-5 text-green-500" /><span className="text-sm">Created: Patient outcomes analytics pipeline</span></div>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </section>

      <section id="features" className="py-20 px-4 sm:px-6 lg:px-8 bg-muted/30">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-3xl font-bold mb-4">Powerful Features</h2>
            <p className="text-muted-foreground max-w-2xl mx-auto">Everything you need to build, manage, and optimize your data pipelines with AI</p>
          </div>
          <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
            {features.map((feature, index) => (
              <Card key={index} className="hover:shadow-lg transition-shadow">
                <CardContent className="p-6">
                  <div className={`w-12 h-12 rounded-lg ${feature.bgColor} flex items-center justify-center mb-4`}>
                    <feature.icon className={`h-6 w-6 ${feature.color}`} />
                  </div>
                  <h3 className="font-semibold text-lg mb-2">{feature.title}</h3>
                  <p className="text-muted-foreground">{feature.description}</p>
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      </section>

      <section id="how-it-works" className="py-20 px-4 sm:px-6 lg:px-8">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-3xl font-bold mb-4">How It Works</h2>
            <p className="text-muted-foreground max-w-2xl mx-auto">Four simple steps to transform your data operations</p>
          </div>
          <div className="grid gap-8 md:grid-cols-4">
            {steps.map((step, index) => (
              <div key={index} className="relative">
                <div className="flex items-center gap-4 mb-4">
                  <div className="w-10 h-10 rounded-full bg-gradient-to-br from-violet-600 to-cyan-500 flex items-center justify-center text-white font-bold">{step.step}</div>
                  {index < steps.length - 1 && (<div className="hidden md:block flex-1 h-0.5 bg-gradient-to-r from-violet-600 to-cyan-500" />)}
                </div>
                <h3 className="font-semibold text-lg mb-2">{step.title}</h3>
                <p className="text-muted-foreground">{step.description}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section id="pricing" className="py-20 px-4 sm:px-6 lg:px-8 bg-muted/30">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-3xl font-bold mb-4">Simple, Transparent <span className="bg-gradient-to-r from-violet-600 to-cyan-500 bg-clip-text text-transparent">Pricing</span></h2>
            <p className="text-muted-foreground max-w-2xl mx-auto">Choose the plan that fits your data needs. Start free and scale as you grow.</p>
          </div>
          <div className="grid gap-8 md:grid-cols-3 items-start">
            <Card className="hover:shadow-lg transition-shadow">
              <CardHeader className="text-center pb-2">
                <CardTitle className="text-xl">Free</CardTitle>
                <CardDescription>For individuals exploring data</CardDescription>
                <div className="mt-4"><span className="text-4xl font-bold">$0</span><span className="text-muted-foreground">/month</span></div>
              </CardHeader>
              <CardContent className="space-y-6">
                <Separator />
                <ul className="space-y-3">{["5 datasets per month","Basic schema detection","Manual pipeline creation","Community support","100MB storage"].map((feature) => (<li key={feature} className="flex items-start gap-3"><CheckCircle2 className="h-5 w-5 text-muted-foreground shrink-0 mt-0.5" /><span className="text-sm text-muted-foreground">{feature}</span></li>))}</ul>
                <Button variant="outline" className="w-full" onClick={() => setAuthDialogOpen(true)}>Get Started</Button>
              </CardContent>
            </Card>
            <Card className="relative border-2 border-violet-500 shadow-xl hover:shadow-2xl transition-shadow md:scale-105">
              <div className="absolute -top-3 left-1/2 -translate-x-1/2"><Badge className="bg-gradient-to-r from-violet-600 to-cyan-500 text-white px-4 py-1 text-xs font-semibold"><Sparkles className="h-3 w-3 mr-1" />MOST POPULAR</Badge></div>
              <CardHeader className="text-center pb-2 pt-6">
                <CardTitle className="text-xl">Pro</CardTitle>
                <CardDescription>For teams building at scale</CardDescription>
                <div className="mt-4"><span className="text-4xl font-bold">$49</span><span className="text-muted-foreground">/month</span></div>
              </CardHeader>
              <CardContent className="space-y-6">
                <Separator />
                <ul className="space-y-3">{["Unlimited datasets","AI-powered analysis (LLM)","Auto dbt model generation","NL→SQL query","10GB storage","Priority support","Airbyte connections (up to 5)"].map((feature) => (<li key={feature} className="flex items-start gap-3"><CheckCircle2 className="h-5 w-5 text-violet-500 shrink-0 mt-0.5" /><span className="text-sm">{feature}</span></li>))}</ul>
                <Button className="w-full bg-gradient-to-r from-violet-600 to-cyan-500 hover:from-violet-700 hover:to-cyan-600" onClick={() => setAuthDialogOpen(true)}>Start Free Trial <ArrowRight className="ml-2 h-4 w-4" /></Button>
              </CardContent>
            </Card>
            <Card className="hover:shadow-lg transition-shadow">
              <CardHeader className="text-center pb-2">
                <CardTitle className="text-xl">Enterprise</CardTitle>
                <CardDescription>For organizations with custom needs</CardDescription>
                <div className="mt-4"><span className="text-4xl font-bold">Custom</span></div>
              </CardHeader>
              <CardContent className="space-y-6">
                <Separator />
                <ul className="space-y-3">{["Everything in Pro","Unlimited storage","Unlimited Airbyte connections","Custom AI model training","Dedicated support","SLA guarantee","On-premise deployment option"].map((feature) => (<li key={feature} className="flex items-start gap-3"><CheckCircle2 className="h-5 w-5 text-muted-foreground shrink-0 mt-0.5" /><span className="text-sm text-muted-foreground">{feature}</span></li>))}</ul>
                <Button variant="outline" className="w-full" onClick={() => setAuthDialogOpen(true)}>Contact Sales</Button>
              </CardContent>
            </Card>
          </div>
        </div>
      </section>

      <footer className="py-12 px-4 sm:px-6 lg:px-8 border-t">
        <div className="max-w-7xl mx-auto">
          <div className="flex flex-col md:flex-row items-center justify-between gap-4">
            <div className="flex items-center gap-2">
              <div className="w-8 h-8 bg-gradient-to-br from-violet-600 to-cyan-500 rounded-lg flex items-center justify-center"><Zap className="w-5 h-5 text-white" /></div>
              <span className="font-bold">DataForge AI v3.0</span>
            </div>
            <p className="text-sm text-muted-foreground">LLM + XLSX + Airbyte — Works with ANY dataset</p>
          </div>
        </div>
      </footer>

      <AuthDialog open={authDialogOpen} onOpenChange={setAuthDialogOpen} onSuccess={onGetStarted} />
    </div>
  );
}
// ============ Downloads Section ============

function DownloadsSection({ schema }: { schema: any }) {
  const [files, setFiles] = useState<FileItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedFile, setSelectedFile] = useState<FileItem | null>(null);
  const [fileContent, setFileContent] = useState<string>("");
  const [downloading, setDownloading] = useState<string | null>(null);

  useEffect(() => {
    async function fetchFiles() {
      try {
        const res = await fetch("/api/files");
        const data = await res.json();
        setFiles(data.files || []);
      } catch {
        setFiles([]);
      } finally {
        setLoading(false);
      }
    }
    fetchFiles();
  }, []);

  const handleViewFile = async (file: FileItem) => {
    setSelectedFile(file);
    setFileContent("Loading...");
    try {
      const res = await fetch(`/api/files?path=${encodeURIComponent(file.path)}`);
      const data = await res.json();
      setFileContent(data.content || "[Binary file — cannot display]");
    } catch {
      setFileContent("Error loading file content.");
    }
  };

  const handleDownload = async (file: FileItem) => {
    setDownloading(file.path);
    try {
      const res = await fetch(`/api/download?path=${encodeURIComponent(file.path)}`);
      if (!res.ok) throw new Error("Download failed");
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = file.name;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch {
      alert("Download failed. The backend may not be running.");
    } finally {
      setDownloading(null);
    }
  };

  const formatSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  const getCategoryIcon = (category: string) => {
    switch (category) {
      case "raw_data": return <Upload className="h-4 w-4 text-orange-500" />;
      case "clean_data": return <CheckCircle2 className="h-4 w-4 text-green-500" />;
      case "pipeline": return <GitBranch className="h-4 w-4 text-blue-500" />;
      case "report": return <FileSpreadsheet className="h-4 w-4 text-purple-500" />;
      case "schema": return <Layers className="h-4 w-4 text-cyan-500" />;
      case "dbt_model": return <Code className="h-4 w-4 text-orange-500" />;
      default: return <FileText className="h-4 w-4 text-gray-500" />;
    }
  };

  const getCategoryBadge = (category: string) => {
    const colors: Record<string, string> = {
      raw_data: "bg-orange-100 text-orange-700 dark:bg-orange-900 dark:text-orange-300",
      clean_data: "bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300",
      pipeline: "bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300",
      report: "bg-purple-100 text-purple-700 dark:bg-purple-900 dark:text-purple-300",
      schema: "bg-cyan-100 text-cyan-700 dark:bg-cyan-900 dark:text-cyan-300",
      dbt_model: "bg-amber-100 text-amber-700 dark:bg-amber-900 dark:text-amber-300",
    };
    return colors[category] || "bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-300";
  };

  if (loading) {
    return (
      <div className="space-y-4">
        <div className="flex items-center gap-2">
          <Download className="h-6 w-6 text-purple-500" />
          <h1 className="text-2xl font-bold">Downloads</h1>
        </div>
        <p className="text-muted-foreground">Download generated files: cleaned data, reports, pipeline code, and more.</p>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {[1, 2, 3, 4, 5, 6].map((i) => (
            <Card key={i}>
              <CardContent className="p-4">
                <div className="animate-pulse space-y-2">
                  <div className="h-4 bg-muted rounded w-3/4" />
                  <div className="h-3 bg-muted rounded w-1/2" />
                  <div className="h-3 bg-muted rounded w-1/3" />
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <div className="flex items-center gap-2">
            <Download className="h-6 w-6 text-purple-500" />
            <h1 className="text-2xl font-bold">Downloads</h1>
          </div>
          <p className="text-muted-foreground">Download generated files: cleaned data, reports, pipeline code, and more.</p>
        </div>
        <Badge variant="secondary">{files.length} files</Badge>
      </div>

      {files.length === 0 ? (
        <Card>
          <CardContent className="p-8 text-center">
            <FolderOpen className="h-12 w-12 text-muted-foreground mx-auto mb-3" />
            <h3 className="text-lg font-semibold mb-1">No files yet</h3>
            <p className="text-muted-foreground">Upload a dataset and use the agent to generate files.</p>
          </CardContent>
        </Card>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {files.map((file) => (
            <Card
              key={file.path}
              className="hover:shadow-md transition-shadow cursor-pointer"
              onClick={() => handleViewFile(file)}
            >
              <CardHeader className="pb-2">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    {getCategoryIcon(file.category)}
                    <CardTitle className="text-sm font-medium truncate max-w-[180px]">
                      {file.name}
                    </CardTitle>
                  </div>
                </div>
                <CardDescription className="text-xs">
                  {file.path}
                </CardDescription>
              </CardHeader>
              <CardContent className="pt-0">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Badge className={`text-xs ${getCategoryBadge(file.category)}`}>
                      {file.category.replace("_", " ")}
                    </Badge>
                    <span className="text-xs text-muted-foreground">{formatSize(file.size)}</span>
                  </div>
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDownload(file);
                    }}
                    disabled={downloading === file.path}
                  >
                    {downloading === file.path ? (
                      <Loader2 className="h-3 w-3 animate-spin" />
                    ) : (
                      <Download className="h-3 w-3" />
                    )}
                  </Button>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* File Preview Dialog */}
      {selectedFile && (
        <Card className="mt-4">
          <CardHeader className="pb-2">
            <div className="flex items-center justify-between">
              <CardTitle className="text-base">
                <Eye className="h-4 w-4 inline mr-2" />
                {selectedFile.name}
              </CardTitle>
              <div className="flex items-center gap-2">
                <Button
                  size="sm"
                  onClick={() => handleDownload(selectedFile)}
                  disabled={downloading === selectedFile.path}
                >
                  {downloading === selectedFile.path ? (
                    <Loader2 className="h-4 w-4 animate-spin mr-1" />
                  ) : (
                    <Download className="h-4 w-4 mr-1" />
                  )}
                  Download
                </Button>
                <Button size="sm" variant="ghost" onClick={() => setSelectedFile(null)}>
                  <X className="h-4 w-4" />
                </Button>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="bg-muted rounded-md p-4 max-h-96 overflow-auto">
              <pre className="text-xs whitespace-pre-wrap font-mono">{fileContent}</pre>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}


// Main App Component
export default function DataForgeApp() {
  const { theme, setTheme } = useTheme();
  const { logout, isAuthenticated, restoreSession } = useAuthStore();
  const [currentView, setCurrentView] = useState<"landing" | "app">("landing");
  const [activeTab, setActiveTab] = useState("home");
  const [mobileSidebarOpen, setMobileSidebarOpen] = useState(false);

  useEffect(() => { restoreSession(); }, []);
  useEffect(() => { if (isAuthenticated) { setCurrentView("app"); } }, [isAuthenticated]);

  const handleLogout = () => {
    logout();
    setCurrentView("landing");
  };
  
  // State
  const [isLoading, setIsLoading] = useState(false);
  const [executionResult, setExecutionResult] = useState<ExecutionResult | null>(null);
  const [files, setFiles] = useState<FileItem[]>([]);
  const [workspaceFiles, setWorkspaceFiles] = useState<FileItem[]>([]);
  // ✅ FIX: Track selected active file
  const [selectedFile, setSelectedFile] = useState<FileItem | null>(null);
  const [stats, setStats] = useState<DashboardStats>({
    total_pipelines: 0, total_executions: 0, success_rate: 95.5, tables: 0, reports: 0, data_volume: 0, dataset_type: 'none', current_table: 'none'
  });
  const [chartData, setChartData] = useState<ChartData>({ pipeline_runs: [], primary_chart: [], secondary_chart: [], trend_chart: [] });
  const [schema, setSchema] = useState<SchemaInfo | null>(null);
  const [pipelineStatus, setPipelineStatus] = useState<"idle" | "running" | "success" | "error">("idle");
  const [llmAnalysis, setLlmAnalysis] = useState<LLMAnalysis | null>(null);
  const [llmLoading, setLlmLoading] = useState(false);
  const [dbtModels, setDbtModels] = useState<Array<{ path: string; content: string; description: string }> | null>(null);
  const [dbtLoading, setDbtLoading] = useState(false);

  useEffect(() => {
    if (currentView === "app") { fetchFiles(); fetchStats(); fetchChartData(); fetchSchema(); }
  }, [currentView]);

  const toRelativePath = (path: string): string => {
    const normalized = path.replaceAll("\\", "/");
    const marker = "/my-project/";
    const idx = normalized.toLowerCase().lastIndexOf(marker);
    if (idx >= 0) {
      return normalized.slice(idx + marker.length);
    }
    const dataIdx = normalized.toLowerCase().indexOf("data/");
    if (dataIdx >= 0) {
      return normalized.slice(dataIdx);
    }
    return normalized;
  };

  const isDataFile = (file: FileItem | null) => {
    if (!file) return false;
    const type = (file.type || "").toLowerCase();
    return ["csv", "json", "xlsx", "xls", "xlsm"].includes(type);
  };

  const clearWorkspaceContext = () => {
    setExecutionResult(null);
    setWorkspaceFiles([]);
    setPipelineStatus("idle");
  };

  const setActiveFileContext = async (file: FileItem) => {
    const filePath = toRelativePath(file.path);
    const response = await fetch("/api/active-file", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ file_path: filePath }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || payload?.status === "error") {
      throw new Error(payload?.message || payload?.error || payload?.detail || "Failed to set active file");
    }
  };

  const handleSelectFile = async (file: FileItem, resetWorkspace = false) => {
    setSelectedFile(file);
    if (resetWorkspace) {
      clearWorkspaceContext();
    }
    if (isDataFile(file)) {
      try {
        await setActiveFileContext(file);
        await fetchSchema();
        await fetchStats();
      } catch (error) {
        console.error("Failed to activate selected file:", error);
      }
    }
  };

  const activateUploadedFile = async (uploadResult?: any) => {
    const uploadedPath = uploadResult?.file_path ? toRelativePath(uploadResult.file_path) : null;
    if (uploadedPath) {
      const uploadedFile: FileItem = {
        name: uploadedPath.split("/").pop() || uploadedPath,
        path: uploadedPath,
        type: uploadedPath.split(".").pop() || "csv",
        category: "raw_data",
        size: 0,
      };
      await handleSelectFile(uploadedFile, true);
      return;
    }

    const latest = await fetch(`/api/files?_=${Date.now()}`).then((r) => r.json()).catch(() => ({ files: [] }));
    const rawFiles: FileItem[] = (latest.files || []).filter((f: FileItem) => f.category === "raw_data");
    if (rawFiles.length > 0) {
      await handleSelectFile(rawFiles[rawFiles.length - 1], true);
    }
  };

  // ✅ FIX: Auto-select first uploaded raw file
  const fetchFiles = async () => {
    try {
      // ✅ FIX: Add timestamp to prevent caching
      const timestamp = new Date().getTime();
      const response = await fetch(`/api/files?_=${timestamp}`);
      const data = await response.json();
      const fetchedFiles = data.files || [];
      setFiles(fetchedFiles);
      // Auto-select first raw_data file if no file selected
      if (!selectedFile) {
        const rawFiles = fetchedFiles.filter((f: FileItem) => f.category === "raw_data");
        if (rawFiles.length > 0) {
          await handleSelectFile(rawFiles[rawFiles.length - 1]);
        }
      }
    } catch (error) {
      console.error("Failed to fetch files:", error);
    }
  };

  const fetchStats = async () => {
    try { const response = await fetch("/api/dashboard"); const data = await response.json(); setStats(data); } catch (error) { console.error("Failed to fetch stats:", error); }
  };

  const fetchChartData = async () => {
    try { const response = await fetch("/api/dashboard/charts"); const data = await response.json(); setChartData(data); } catch (error) { console.error("Failed to fetch chart data:", error); }
  };

  const fetchSchema = async () => {
    try {
      const response = await fetch("/api/schema"); const data = await response.json();
      if (data.schema && data.schema.columns && Object.keys(data.schema.columns).length > 0) { setSchema(data.schema); } else { setSchema(null); }
    } catch (error) { console.error("Failed to fetch schema:", error); setSchema(null); }
  };

  const handleLLMAnalysis = async () => {
    setLlmLoading(true);
    try {
      const response = await fetch("/api/llm/analyze", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ active_file: isDataFile(selectedFile) ? toRelativePath(selectedFile!.path) : null }),
      });
      const text = await response.text();
      let data: any;
      try { data = JSON.parse(text); } catch { console.error("LLM invalid JSON:", text.slice(0, 200)); return; }
      setLlmAnalysis(data.analysis);
    } catch (error) { console.error("LLM analysis failed:", error); } finally { setLlmLoading(false); }
  };

  const handleGenerateDBT = async () => {
    setDbtLoading(true);
    try {
      const response = await fetch("/api/llm/generate-dbt", { method: "POST" });
      const text = await response.text();
      let data: any;
      try { data = JSON.parse(text); } catch { console.error("dbt invalid JSON:", text.slice(0, 200)); return; }
      if (data.status === "success" && data.models) { setDbtModels(data.models); fetchFiles(); }
      else { console.error("dbt generation error:", data.message || data.error || "Unknown error"); }
    } catch (error) { console.error("dbt generation failed:", error); } finally { setDbtLoading(false); }
  };

  const handleExecuteCommand = async (command: string) => {
    setIsLoading(true); 
    setPipelineStatus("running");
    setWorkspaceFiles([]);
    
    try {
      const response = await fetch("/api/agent", { 
        method: "POST", 
        headers: { "Content-Type": "application/json" }, 
        body: JSON.stringify({ command, active_file: isDataFile(selectedFile) ? toRelativePath(selectedFile!.path) : null }) 
      });
      
      const result = await response.json();
      const normalized = (result?.data && result?.status === "success")
        ? ({ ...result.data, status: result.data.status || "success", message: result.message || result.data.message } as ExecutionResult)
        : (result as ExecutionResult);

      setExecutionResult(normalized);
      setPipelineStatus(normalized.status === "success" ? "success" : "error");
      
      // ✅ FIX: Increased timeout and sequential refresh for successful executions
      if (normalized.status === "success") {
        setTimeout(async () => { 
          await fetchFiles(); 
          await fetchStats(); 
          await fetchChartData(); 
          await fetchSchema(); 
          const generatedSet = new Set((normalized.files || []).map((p) => toRelativePath(p)));
          if (generatedSet.size > 0) {
            const refreshed = await fetch(`/api/files?_=${Date.now()}`).then((r) => r.json()).catch(() => ({ files: [] }));
            const matched = (refreshed.files || []).filter((f: FileItem) => generatedSet.has(toRelativePath(f.path)));
            setWorkspaceFiles(matched);
          } else {
            setWorkspaceFiles([]);
          }
        }, 2000); // Increased to 2 seconds to allow file system writes
      }
    } catch (error) { 
      console.error("Execution error:", error); 
      setPipelineStatus("error"); 
    } finally { 
      setIsLoading(false); 
    }
  };

  const handleQuery = async (question: string): Promise<QueryResult> => {
    const response = await fetch("/api/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question, active_file: isDataFile(selectedFile) ? toRelativePath(selectedFile!.path) : null }),
    });
    const result = await response.json();
    if (result.schema_info) { setSchema(result.schema_info); }
    return result;
  };

  const sidebarItems = [
    { id: "home", label: "Home", icon: Home },
    { id: "upload", label: "Upload Data", icon: Upload },
    { id: "schema", label: "Schema", icon: Table },
    { id: "analysis", label: "AI Analysis", icon: Bot },
    { id: "agent", label: "Agent Workspace", icon: Zap },
    { id: "pipelines", label: "Pipelines", icon: GitBranch },
    { id: "downloads", label: "Downloads", icon: Download },
    { id: "dbt", label: "dbt Models", icon: Code },
    { id: "airbyte", label: "Data Sources", icon: Cloud },
    { id: "warehouse", label: "Warehouse", icon: Database },
    { id: "reports", label: "Reports", icon: BarChart3 },
    { id: "query", label: "Query Data", icon: FileText },
    { id: "files", label: "Files", icon: FolderOpen },
  ];

  if (currentView === "landing") { return <LandingPage onGetStarted={() => setCurrentView("app")} />; }

  return (
    <div className="min-h-screen flex flex-col bg-background">
      <header className="lg:hidden sticky top-0 z-50 bg-background border-b">
        <div className="flex items-center justify-between h-14 px-4">
          <Button variant="ghost" size="icon" onClick={() => setMobileSidebarOpen(!mobileSidebarOpen)}><Menu className="h-5 w-5" /></Button>
          <div className="flex items-center gap-2">
            <div className="w-8 h-8 bg-gradient-to-br from-violet-600 to-cyan-500 rounded-lg flex items-center justify-center"><Zap className="w-5 w-5 text-white" /></div>
            <span className="font-bold">DataForge</span>
            {selectedFile && <Badge variant="outline" className="text-xs max-w-[100px] truncate">{selectedFile.name}</Badge>}
            {stats.dataset_type && stats.dataset_type !== 'none' && !selectedFile && <Badge variant="outline" className="text-xs">{stats.dataset_type}</Badge>}
          </div>
          <Button variant="ghost" size="icon" onClick={() => setTheme(theme === "dark" ? "light" : "dark")}>
            {theme === "dark" ? <Sun className="h-5 w-5" /> : <Moon className="h-5 w-5" />}
          </Button>
        </div>
      </header>

      {mobileSidebarOpen && (<div className="lg:hidden fixed inset-0 z-50 bg-black/50" onClick={() => setMobileSidebarOpen(false)} />)}

      <aside className={`lg:hidden fixed top-0 left-0 z-50 h-full w-64 bg-background border-r transform transition-transform ${mobileSidebarOpen ? "translate-x-0" : "-translate-x-full"}`}>
        <div className="flex flex-col h-full">
          <div className="flex items-center gap-2 h-14 px-4 border-b">
            <div className="w-8 h-8 bg-gradient-to-br from-violet-600 to-cyan-500 rounded-lg flex items-center justify-center"><Zap className="w-5 h-5 text-white" /></div>
            <span className="font-bold">DataForge AI</span>
          </div>
          <ScrollArea className="flex-1 p-2">
            <nav className="space-y-1">
              {sidebarItems.map((item) => (
                <Button key={item.id} variant={activeTab === item.id ? "secondary" : "ghost"} className="w-full justify-start gap-3" onClick={() => { setActiveTab(item.id); setMobileSidebarOpen(false); }}>
                  <item.icon className="h-5 w-5" />{item.label}
                </Button>
              ))}
            </nav>
          </ScrollArea>
          <div className="p-2 border-t">
            <Button variant="ghost" className="w-full justify-start gap-3 text-red-500" onClick={handleLogout}><LogOut className="h-5 w-5" />Logout</Button>
          </div>
        </div>
      </aside>

      <div className="flex flex-1">
        <aside className="hidden lg:flex flex-col w-64 border-r bg-background">
          <div className="flex items-center gap-2 h-14 px-4 border-b">
            <div className="w-8 h-8 bg-gradient-to-br from-violet-600 to-cyan-500 rounded-lg flex items-center justify-center"><Zap className="w-5 h-5 text-white" /></div>
            <span className="font-bold">DataForge AI</span>
          </div>
          <ScrollArea className="flex-1 p-2">
            <nav className="space-y-1">
              {sidebarItems.map((item) => (
                <Button key={item.id} variant={activeTab === item.id ? "secondary" : "ghost"} className="w-full justify-start gap-3" onClick={() => setActiveTab(item.id)}>
                  <item.icon className="h-5 w-5" />{item.label}
                </Button>
              ))}
            </nav>
          </ScrollArea>
          <div className="p-2 border-t space-y-1">
            {selectedFile && (
              <div className="px-3 py-2 text-xs text-muted-foreground flex items-center gap-1">
                <FileSpreadsheet className="h-3 w-3 text-green-500" />
                <span className="truncate">{selectedFile.name}</span>
              </div>
            )}
            {stats.dataset_type && stats.dataset_type !== 'none' && !selectedFile && (
              <div className="px-3 py-2 text-xs text-muted-foreground">Current Dataset: <Badge variant="outline">{stats.dataset_type}</Badge></div>
            )}
            {stats.features && (
              <div className="px-3 py-1 text-xs text-muted-foreground">
                <div className="flex items-center gap-1"><Bot className="h-3 w-3" />LLM: {stats.features.llm_enabled ? "✓" : "✗"}</div>
                <div className="flex items-center gap-1"><FileSpreadsheet className="h-3 w-3" />XLSX: {stats.features.xlsx_support ? "✓" : "✗"}</div>
                <div className="flex items-center gap-1"><Cloud className="h-3 w-3" />Airbyte: {stats.features.airbyte_connected ? "✓" : "✗"}</div>
              </div>
            )}
            <Button variant="ghost" className="w-full justify-start gap-3" onClick={() => setTheme(theme === "dark" ? "light" : "dark")}>
              {theme === "dark" ? <Sun className="h-5 w-5" /> : <Moon className="h-5 w-5" />}{theme === "dark" ? "Light Mode" : "Dark Mode"}
            </Button>
            <Button variant="ghost" className="w-full justify-start gap-3 text-red-500 hover:text-red-600" onClick={handleLogout}><LogOut className="h-5 w-5" />Logout</Button>
          </div>
        </aside>

        <main className="flex-1 overflow-auto">
          <ScrollArea className="h-[calc(100vh-3.5rem)] lg:h-screen">
            <div className="p-4 md:p-6 lg:p-8 space-y-6">

              {/* ✅ FIX: Active File Bar - shows on EVERY tab */}
              {selectedFile && (
                <ActiveFileBar file={selectedFile} onClear={() => setSelectedFile(null)} onPreview={() => setActiveTab("files")} />
              )}

              {/* Home Tab */}
              {activeTab === "home" && (
                <>
                  <div className="flex items-center justify-between">
                    <div>
                      <h1 className="text-2xl font-bold">Welcome back! 👋</h1>
                      <p className="text-muted-foreground">
                        {selectedFile ? `Working with ${selectedFile.name}` : schema ? `Working with ${schema.dataset_type} dataset` : 'Upload a dataset to get started'}
                      </p>
                    </div>
                  </div>

                  {!schema ? (
                    <Card className="border-dashed">
                      <CardHeader>
                        <CardTitle className="flex items-center gap-2"><Upload className="h-5 w-5" />Get Started</CardTitle>
                        <CardDescription>Upload a CSV, JSON, or Excel file, or try a sample dataset</CardDescription>
                      </CardHeader>
                      <CardContent className="space-y-4">
                        <FileUploader onUploadComplete={async (uploadResult) => {
                          clearWorkspaceContext();
                          await fetchFiles();
                          await fetchSchema();
                          await fetchStats();
                          await fetchChartData();
                          await activateUploadedFile(uploadResult);
                        }} />
                        <div className="flex items-center gap-2"><Separator className="flex-1" /><span className="text-sm text-muted-foreground">or try sample data</span><Separator className="flex-1" /></div>
                        {/* ✅ FIX: Hide samples when file is uploaded */}
                        {!selectedFile && (
                          <SampleDatasets onSelect={async (name: string) => {
                            await handleSelectFile({ name, path: `data/raw/${name}`, type: "csv", category: "raw_data", size: 0 }, true);
                            await fetchSchema();
                            await fetchStats();
                            await fetchChartData();
                          }} />
                        )}
                      </CardContent>
                    </Card>
                  ) : (
                    <>
                      <div className="flex items-center gap-2">
                        <Button onClick={handleLLMAnalysis} disabled={llmLoading}>{llmLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Bot className="h-4 w-4 mr-2" />}Analyze with AI</Button>
                        <Button onClick={handleGenerateDBT} disabled={dbtLoading} variant="outline">{dbtLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Code className="h-4 w-4 mr-2" />}Generate dbt Models</Button>
                      </div>
                      <CommandBox onExecute={handleExecuteCommand} isLoading={isLoading} />
                      {executionResult && (
                        <Card>
                          <CardHeader>
                            <CardTitle className="flex items-center gap-2">{executionResult.status === "success" ? <CheckCircle2 className="h-5 w-5 text-green-500" /> : <X className="h-5 w-5 text-red-500" />}Execution Result</CardTitle>
                          </CardHeader>
                          <CardContent className="space-y-4">
                            <div className="flex items-center gap-4 text-sm">
                              <Badge variant={executionResult.status === "success" ? "default" : "destructive"}>{executionResult.status}</Badge>
                              {executionResult.duration && (<span className="text-muted-foreground">Completed in {executionResult.duration.toFixed(2)}s</span>)}
                            </div>
                            {executionResult.logs && executionResult.logs.length > 0 && (
                              <div className="bg-muted rounded-lg p-4">
                                <p className="text-sm font-medium mb-2">Execution Logs:</p>
                                <div className="space-y-1 max-h-40 overflow-y-auto">{executionResult.logs.map((log, i) => (<p key={i} className="text-sm text-muted-foreground font-mono">{log}</p>))}</div>
                              </div>
                            )}
                            {executionResult.files && executionResult.files.length > 0 && (
                              <div>
                                <p className="text-sm font-medium mb-2">Generated Files:</p>
                                <div className="grid gap-2 md:grid-cols-2">{executionResult.files.map((file, i) => (<Badge key={i} variant="secondary">{file}</Badge>))}</div>
                              </div>
                            )}
                          </CardContent>
                        </Card>
                      )}
                    </>
                  )}
                  <StatsCards stats={stats} />
                </>
              )}

              {/* Upload Tab */}
              {activeTab === "upload" && (
                <>
                  <div className="flex items-center gap-2"><Upload className="h-6 w-6 text-violet-500" /><h1 className="text-2xl font-bold">Upload Data</h1></div>
                  <p className="text-muted-foreground">Upload any CSV, JSON, or Excel file. The system will automatically detect the schema.</p>
                  <FileUploader onUploadComplete={async (uploadResult) => {
                    clearWorkspaceContext();
                    await fetchFiles();
                    await fetchSchema();
                    await fetchStats();
                    await fetchChartData();
                    await activateUploadedFile(uploadResult);
                  }} />
                  <div className="flex items-center gap-2"><Separator className="flex-1" /><span className="text-sm text-muted-foreground">or try sample datasets</span><Separator className="flex-1" /></div>
                  {/* ✅ FIX: Hide samples when file is uploaded */}
                  {!selectedFile && (
                    <SampleDatasets onSelect={async (name: string) => {
                      await handleSelectFile({ name, path: `data/raw/${name}`, type: "csv", category: "raw_data", size: 0 }, true);
                      await fetchSchema();
                      await fetchStats();
                      await fetchChartData();
                    }} />
                  )}
                </>
              )}

              {/* Schema Tab */}
              {activeTab === "schema" && (
                <>
                  <div className="flex items-center gap-2"><Table className="h-6 w-6 text-blue-500" /><h1 className="text-2xl font-bold">Detected Schema</h1></div>
                  <p className="text-muted-foreground">Schema is automatically detected from your uploaded data.</p>
                  <SchemaDisplay schema={schema} />
                </>
              )}

              {/* AI Analysis Tab */}
              {activeTab === "analysis" && (
                <>
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2"><Bot className="h-6 w-6 text-violet-500" /><h1 className="text-2xl font-bold">AI Analysis</h1></div>
                    <Button onClick={handleLLMAnalysis} disabled={llmLoading || !schema}>{llmLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Sparkles className="h-4 w-4 mr-2" />}Analyze with AI</Button>
                  </div>
                  <p className="text-muted-foreground">LLM-powered intelligent analysis of your dataset.</p>
                  <LLMAnalysisDisplay analysis={llmAnalysis} isLoading={llmLoading} />
                </>
              )}

              {/* Agent Tab */}
              {activeTab === "agent" && (
                <>
                  <div className="flex items-center gap-2"><Zap className="h-6 w-6 text-violet-500" /><h1 className="text-2xl font-bold">Agent Workspace</h1></div>
                  <p className="text-muted-foreground">Tell the AI agent what to do — ingest, analyze, query, generate dbt models, run pipelines, and more.</p>
                  {schema ? (
                    <div className="flex items-center gap-2 text-sm text-muted-foreground"><Info className="h-4 w-4" />Working with <Badge variant="outline">{schema.dataset_type}</Badge> dataset — {Object.keys(schema.columns).length} columns detected</div>
                  ) : (
                    <Card className="border border-amber-500/30 bg-amber-500/5">
                      <CardContent className="flex items-center gap-3 py-3 px-4"><Upload className="h-5 w-5 text-amber-500 shrink-0" /><p className="text-sm text-amber-700 dark:text-amber-400">No dataset loaded. <Button variant="link" className="h-auto p-0 text-amber-700 dark:text-amber-400 underline" onClick={() => setActiveTab("upload")}>Upload data</Button> first, or use the commands below to get started.</p></CardContent>
                    </Card>
                  )}
                  <CommandBox onExecute={handleExecuteCommand} isLoading={isLoading} suggestedCommands={schema ? [`Analyze the ${schema.dataset_type} dataset`,"Run full data pipeline","Generate dbt transformation models","Show top 10 records"] : undefined} />
                  {executionResult && (
                    <Card className="shadow-lg">
                      <CardHeader className="pb-3">
                        <div className="flex items-center justify-between"><CardTitle className="flex items-center gap-2"><Play className="h-5 w-5 text-green-500" />Agent Output</CardTitle><Badge variant={executionResult.status === "success" ? "default" : "destructive"}>{executionResult.status}</Badge></div>
                        <CardDescription>Files generated by DataForge Agent</CardDescription>
                      </CardHeader>
                      <CardContent>
                        {executionResult.logs && executionResult.logs.length > 0 && (
                          <div className="mb-4 p-3 bg-muted rounded-lg max-h-48 overflow-y-auto"><p className="text-xs font-semibold text-muted-foreground mb-2">Execution Log</p>{executionResult.logs.map((log, i) => (<p key={i} className="text-xs font-mono text-muted-foreground">{log}</p>))}</div>
                        )}
                        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                          {workspaceFiles.map((file, i) => (<FileCard key={i} file={file} />))}
                          {workspaceFiles.length === 0 && isLoading && (<><FileCardSkeleton /><FileCardSkeleton /><FileCardSkeleton /></>)}
                        </div>
                      </CardContent>
                    </Card>
                  )}
                </>
              )}

              {/* Pipelines Tab */}
              
              {activeTab === "pipelines" && (
                <>
                  <div className="flex items-center gap-2"><GitBranch className="h-6 w-6 text-blue-500" /><h1 className="text-2xl font-bold">Pipelines</h1></div>
                  <PipelineDAG status={pipelineStatus} onRun={handleExecuteCommand.bind(null, "run pipeline")} isRunning={isLoading} />
                </>
              )}
                            {/* Downloads Tab */}
              {activeTab === "downloads" && (
                <DownloadsSection schema={schema} />
              )}
              
              {/* dbt Models Tab */}
              {activeTab === "dbt" && (
                <>
                  <div className="flex items-center justify-between"><div className="flex items-center gap-2"><Code className="h-6 w-6 text-orange-500" /><h1 className="text-2xl font-bold">dbt Models</h1></div><Button onClick={handleGenerateDBT} disabled={dbtLoading || !schema}>{dbtLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Sparkles className="h-4 w-4 mr-2" />}Generate dbt Models</Button></div>
                  <p className="text-muted-foreground">AI-generated dbt transformation models for your data.</p>
                  <DBTModelsDisplay models={dbtModels} isLoading={dbtLoading} />
                </>
              )}

              {/* Airbyte Tab */}
              {activeTab === "airbyte" && (
                <>
                  <div className="flex items-center gap-2"><Cloud className="h-6 w-6 text-cyan-500" /><h1 className="text-2xl font-bold">Data Sources</h1></div>
                  <p className="text-muted-foreground">Connect to 300+ data sources via Airbyte integration.</p>
                  <AirbyteConnectorManager />
                </>
              )}

              {/* Warehouse Tab */}
              {activeTab === "warehouse" && <WarehouseView />}

              {/* Reports Tab */}
              {activeTab === "reports" && (
                <>
                  <div className="flex items-center gap-2"><BarChart3 className="h-6 w-6 text-purple-500" /><h1 className="text-2xl font-bold">Reports</h1></div>
                  <p className="text-muted-foreground">View generated reports and analytics outputs.</p>
                  <FileExplorer filterCategory="report" onSelectFile={(file) => handleSelectFile(file)} activeFilePath={selectedFile?.path} />
                </>
              )}

              {/* Query Tab */}
              {activeTab === "query" && (
                <>
                  <div className="flex items-center gap-2"><FileText className="h-6 w-6 text-amber-500" /><h1 className="text-2xl font-bold">Query Data</h1></div>
                  <p className="text-muted-foreground">Ask questions in plain English — the AI converts them to SQL and runs them against your data warehouse.</p>
                  {!schema && (
                    <Card className="border border-amber-500/30 bg-amber-500/5">
                      <CardContent className="flex items-center gap-3 py-3 px-4"><Upload className="h-5 w-5 text-amber-500 shrink-0" /><p className="text-sm text-amber-700 dark:text-amber-400">No dataset loaded yet. <Button variant="link" className="h-auto p-0 text-amber-700 dark:text-amber-400 underline" onClick={() => setActiveTab("upload")}>Upload data</Button> to enable AI-powered queries.</p></CardContent>
                    </Card>
                  )}
                  <QueryBox onQuery={handleQuery} activeFileName={selectedFile?.name || null} />
                  {schema && schema.suggested_queries && schema.suggested_queries.length > 0 && (
                    <Card className="shadow-lg">
                      <CardHeader className="pb-3"><CardTitle className="text-base flex items-center gap-2"><Sparkles className="h-4 w-4 text-violet-500" />AI-Suggested Queries for Your {schema.dataset_type.charAt(0).toUpperCase() + schema.dataset_type.slice(1)} Data</CardTitle><CardDescription>Click any suggestion to run it instantly</CardDescription></CardHeader>
                      <CardContent>
                        <div className="grid gap-3 md:grid-cols-2">{schema.suggested_queries.map((query, i) => (<Card key={i} className="cursor-pointer hover:border-primary/50 hover:shadow-md transition-all p-4 border" onClick={() => handleQuery(query)}><div className="flex items-center gap-3"><div className="p-2 rounded-lg bg-violet-500/10"><Search className="h-4 w-4 text-violet-500" /></div><p className="text-sm font-medium">{query}</p></div></Card>))}</div>
                      </CardContent>
                    </Card>
                  )}
                </>
              )}

              {/* Files Tab */}
              {activeTab === "files" && (
                <>
                  <div className="flex items-center gap-2"><FolderOpen className="h-6 w-6 text-teal-500" /><h1 className="text-2xl font-bold">All Files</h1></div>
                  {/* ✅ FIX: Pass props so clicking a file sets it as active */}
                  <FileExplorer onSelectFile={(file) => handleSelectFile(file, true)} activeFilePath={selectedFile?.path} />
                </>
              )}
            </div>
          </ScrollArea>
        </main>
      </div>
    </div>
  );
}
