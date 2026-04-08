"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { 
  Search, 
  Loader2, 
  ChevronDown, 
  ChevronRight,
  Download,
  BarChart3
} from "lucide-react";

interface QueryResult {
  sql: string;
  columns: string[];
  data: Record<string, unknown>[];
  row_count: number;
  execution_time?: number;
}

interface QueryBoxProps {
  onQuery: (question: string) => Promise<QueryResult>;
  isLoading?: boolean;
  activeFileName?: string | null;
}

const exampleQueries = [
  "Top 5 products by sales",
  "Sales by region",
  "Monthly revenue trend",
  "Average sales per product",
];

export function QueryBox({ onQuery, isLoading = false, activeFileName = null }: QueryBoxProps) {
  const [question, setQuestion] = useState("");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [showSQL, setShowSQL] = useState(false);
  const [loading, setLoading] = useState(false);

  const handleQuery = async () => {
    if (!question.trim()) return;
    
    setLoading(true);
    try {
      const res = await onQuery(question.trim());
      const safeResult: QueryResult = {
        sql: typeof res?.sql === "string" ? res.sql : "",
        columns: Array.isArray(res?.columns) ? res.columns : [],
        data: Array.isArray(res?.data) ? res.data : [],
        row_count: typeof res?.row_count === "number"
          ? res.row_count
          : (Array.isArray(res?.data) ? res.data.length : 0),
        execution_time: typeof res?.execution_time === "number" ? res.execution_time : undefined,
      };
      setResult(safeResult);
    } catch (error) {
      console.error("Query error:", error);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
      handleQuery();
    }
  };

  const exportCSV = () => {
    if (!result?.data) return;
    
    const headers = result.columns.join(",");
    const rows = result.data.map(row => 
      result.columns.map(col => row[col]).join(",")
    ).join("\n");
    
    const csv = `${headers}\n${rows}`;
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "query_result.csv";
    a.click();
  };

  return (
    <div className="space-y-6">
      {/* Query Input */}
      <Card className="shadow-lg border-2 border-primary/10">
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-xl">
            <Search className="h-5 w-5 text-violet-500" />
            Ask Your Data
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <Textarea
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Example: Top 5 products by sales revenue"
            className="min-h-[100px] resize-none text-base"
            disabled={loading || isLoading}
          />

          <div className="flex flex-wrap gap-2">
            <span className="text-sm text-muted-foreground">Examples:</span>
            {activeFileName && (
              <Badge variant="outline" className="text-xs">
                Active file: {activeFileName}
              </Badge>
            )}
            {exampleQueries.map((q, i) => (
              <Badge
                key={i}
                variant="secondary"
                className="cursor-pointer hover:bg-secondary/80 transition-colors"
                onClick={() => setQuestion(q)}
              >
                {q}
              </Badge>
            ))}
          </div>

          <Button
            onClick={handleQuery}
            disabled={!question.trim() || loading || isLoading}
            className="w-full bg-gradient-to-r from-violet-600 to-cyan-500 hover:from-violet-700 hover:to-cyan-600 h-11"
          >
            {loading || isLoading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Processing...
              </>
            ) : (
              <>
                <Search className="mr-2 h-4 w-4" />
                Run Query
              </>
            )}
          </Button>
        </CardContent>
      </Card>

      {/* Results */}
      {result && (
        <Card className="shadow-lg">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="flex items-center gap-2">
                <BarChart3 className="h-5 w-5 text-green-500" />
                Query Results
              </CardTitle>
              <div className="flex items-center gap-2">
                <Badge variant="secondary">
                  {result.row_count} rows
                </Badge>
                {result.execution_time && (
                  <Badge variant="outline">
                    {result.execution_time}s
                  </Badge>
                )}
                <Button variant="outline" size="sm" onClick={exportCSV}>
                  <Download className="h-4 w-4 mr-1" />
                  Export CSV
                </Button>
              </div>
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* SQL Preview */}
            <Collapsible open={showSQL} onOpenChange={setShowSQL}>
              <CollapsibleTrigger asChild>
                <Button variant="ghost" size="sm" className="w-full justify-start">
                  {showSQL ? (
                    <ChevronDown className="h-4 w-4 mr-2" />
                  ) : (
                    <ChevronRight className="h-4 w-4 mr-2" />
                  )}
                  View Generated SQL
                </Button>
              </CollapsibleTrigger>
              <CollapsibleContent>
                <pre className="mt-2 p-4 bg-muted rounded-lg text-sm overflow-x-auto">
                  <code>{result.sql}</code>
                </pre>
              </CollapsibleContent>
            </Collapsible>

            {/* Data Table */}
            <div className="rounded-lg border">
              <div className="max-h-96 overflow-y-auto overflow-x-auto scroll-smooth">
                <Table className="min-w-max">
                  <TableHeader className="sticky top-0 bg-muted">
                    <TableRow>
                      {result.columns.map((col) => (
                        <TableHead key={col} className="font-semibold">
                          {col}
                        </TableHead>
                      ))}
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {result.data.length === 0 ? (
                      <TableRow>
                        <TableCell
                          colSpan={result.columns.length}
                          className="text-center text-muted-foreground py-8"
                        >
                          No results found
                        </TableCell>
                      </TableRow>
                    ) : (
                      result.data.map((row, i) => (
                        <TableRow key={i}>
                          {result.columns.map((col) => (
                            <TableCell key={col}>
                              {row[col]?.toString() || "-"}
                            </TableCell>
                          ))}
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </div>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
