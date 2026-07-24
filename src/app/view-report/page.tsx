"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  PieChart,
  Pie,
  Cell,
  LineChart,
  Line,
  CartesianGrid,
  AreaChart,
  Area,
  ScatterChart,
  Scatter,
  ZAxis,
} from "recharts";
import { ArrowLeft, Filter } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

interface ReportPayload {
  status: string;
  table: string;
  total_rows: number;
  columns: Array<{ name: string; type: string }>;
  rows: Array<Record<string, unknown>>;
  charts: {
    bar: Array<{ category: string; value: number }>;
    line: Array<{ period: string; value: number }>;
    pie: Array<{ name: string; value: number }>;
    histogram: Array<{ bucket: string; count: number }>;
    heatmap: Array<{ x: string; y: string; value: number }>;
  };
  bar_options?: {
    selected_category?: string;
    selected_metric?: string;
    category_candidates?: string[];
    metric_candidates?: string[];
  };
}

const PIE_COLORS = ["#8b5cf6", "#06b6d4", "#22c55e", "#f59e0b", "#ef4444", "#6366f1"];

export default function ViewReportPage() {
  const [report, setReport] = useState<ReportPayload | null>(null);
  const [filterColumn, setFilterColumn] = useState<string>("");
  const [filterValue, setFilterValue] = useState<string>("");
  const [sortBy, setSortBy] = useState<string>("");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [barCategory, setBarCategory] = useState<string>("");
  const [barMetric, setBarMetric] = useState<string>("__count__");
  const [drillRows, setDrillRows] = useState<Array<Record<string, unknown>>>([]);

  const loadReport = async () => {
    const params = new URLSearchParams();
    if (filterColumn) params.set("filter_column", filterColumn);
    if (filterValue) params.set("filter_value", filterValue);
    if (sortBy) params.set("sort_by", sortBy);
    if (sortDir) params.set("sort_dir", sortDir);
    if (barCategory) params.set("bar_category", barCategory);
    if (barMetric) params.set("bar_metric", barMetric);

    const response = await fetch(`/api/report/view?${params.toString()}`);
    const data = await response.json();
    setReport(data);
    if (!barCategory && data?.bar_options?.selected_category) {
      setBarCategory(String(data.bar_options.selected_category));
    }
    if ((!barMetric || barMetric === "__count__") && data?.bar_options?.selected_metric) {
      setBarMetric(String(data.bar_options.selected_metric));
    }
  };

  useEffect(() => {
    const timer = window.setTimeout(() => {
      void loadReport();
    }, 0);
    return () => window.clearTimeout(timer);
  }, []);

  const tableColumns = useMemo(() => report?.columns || [], [report]);
  const barCategoryOptions = useMemo(
    () => report?.bar_options?.category_candidates || tableColumns.map((c) => c.name),
    [report, tableColumns]
  );
  const barMetricOptions = useMemo(
    () => report?.bar_options?.metric_candidates || ["__count__"],
    [report]
  );

  const handleDrillDown = async (category: string) => {
    if (!report || tableColumns.length === 0) return;
    const groupBy = barCategory || report.bar_options?.selected_category || tableColumns[0]?.name;
    if (!groupBy) return;

    const params = new URLSearchParams({
      group_by: groupBy,
      group_value: category,
      limit: "80",
    });

    const response = await fetch(`/api/report/drilldown?${params.toString()}`);
    const data = await response.json();
    setDrillRows(data.rows || []);
  };

  return (
    <div className="min-h-screen bg-background">
      <div className="border-b bg-background/95 backdrop-blur">
        <div className="mx-auto flex h-14 max-w-7xl items-center gap-3 px-4">
          <Link href="/" className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground">
            <ArrowLeft className="h-4 w-4" />
            Back
          </Link>
          <div className="h-4 w-px bg-border" />
          <h1 className="font-semibold">View Report</h1>
        </div>
      </div>

      <div className="mx-auto max-w-7xl space-y-6 p-6">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><Filter className="h-4 w-4" />Filters, Sorting, Drill-down</CardTitle>
            <CardDescription>Apply filters and click chart segments to inspect details.</CardDescription>
          </CardHeader>
          <CardContent className="grid gap-2 md:grid-cols-3 xl:grid-cols-7">
            <Select value={filterColumn || "none"} onValueChange={(v) => setFilterColumn(v === "none" ? "" : v)}>
              <SelectTrigger className="h-9"><SelectValue placeholder="Filter column" /></SelectTrigger>
              <SelectContent>
                <SelectItem value="none">No filter</SelectItem>
                {tableColumns.map((c) => <SelectItem key={c.name} value={c.name}>{c.name}</SelectItem>)}
              </SelectContent>
            </Select>
            <Input className="h-9" placeholder="Filter value (e.g. >50)" value={filterValue} onChange={(e) => setFilterValue(e.target.value)} />
            <Select value={sortBy || "none"} onValueChange={(v) => setSortBy(v === "none" ? "" : v)}>
              <SelectTrigger className="h-9"><SelectValue placeholder="Sort by" /></SelectTrigger>
              <SelectContent>
                <SelectItem value="none">No sorting</SelectItem>
                {tableColumns.map((c) => <SelectItem key={c.name} value={c.name}>{c.name}</SelectItem>)}
              </SelectContent>
            </Select>
            <Select value={sortDir} onValueChange={(v: "asc" | "desc") => setSortDir(v)}>
              <SelectTrigger className="h-9"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="asc">Ascending</SelectItem>
                <SelectItem value="desc">Descending</SelectItem>
              </SelectContent>
            </Select>
            <Select value={barCategory || "none"} onValueChange={(v) => setBarCategory(v === "none" ? "" : v)}>
              <SelectTrigger className="h-9"><SelectValue placeholder="Bar category" /></SelectTrigger>
              <SelectContent>
                <SelectItem value="none">Auto category</SelectItem>
                {barCategoryOptions.map((c) => <SelectItem key={c} value={c}>{c}</SelectItem>)}
              </SelectContent>
            </Select>
            <Select value={barMetric || "__count__"} onValueChange={(v) => setBarMetric(v)}>
              <SelectTrigger className="h-9"><SelectValue placeholder="Bar metric" /></SelectTrigger>
              <SelectContent>
                {barMetricOptions.map((m) => (
                  <SelectItem key={m} value={m}>
                    {m === "__count__" ? "Count" : m}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button className="h-9" onClick={loadReport}>Apply</Button>
          </CardContent>
        </Card>

        <div className="grid gap-4 lg:grid-cols-2">
          <Card>
            <CardHeader><CardTitle>Bar</CardTitle></CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={report?.charts.bar || []}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="category" hide />
                  <YAxis />
                  <Tooltip />
                  <Bar dataKey="value" fill="#8b5cf6" onClick={(d) => d?.category && handleDrillDown(String(d.category))} />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle>Line</CardTitle></CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={report?.charts.line || []}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="period" />
                  <YAxis />
                  <Tooltip />
                  <Line type="monotone" dataKey="value" stroke="#06b6d4" strokeWidth={2} />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle>Pie</CardTitle></CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie data={report?.charts.pie || []} dataKey="value" nameKey="name" outerRadius={90} label>
                    {(report?.charts.pie || []).map((entry, idx) => <Cell key={`${entry.name}-${idx}`} fill={PIE_COLORS[idx % PIE_COLORS.length]} />)}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle>Histogram</CardTitle></CardHeader>
            <CardContent className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={report?.charts.histogram || []}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="bucket" hide />
                  <YAxis />
                  <Tooltip />
                  <Area type="monotone" dataKey="count" stroke="#22c55e" fill="#86efac" />
                </AreaChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardHeader><CardTitle>Correlation Heatmap</CardTitle></CardHeader>
          <CardContent className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <ScatterChart>
                <CartesianGrid />
                <XAxis type="category" dataKey="x" allowDuplicatedCategory={false} />
                <YAxis type="category" dataKey="y" allowDuplicatedCategory={false} />
                <ZAxis type="number" dataKey="value" range={[40, 260]} />
                <Tooltip />
                <Scatter data={report?.charts.heatmap || []} fill="#f59e0b" />
              </ScatterChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle>Rows ({report?.total_rows || 0})</CardTitle></CardHeader>
          <CardContent className="overflow-auto">
            <table className="w-full min-w-[900px] text-sm">
              <thead>
                <tr className="border-b">
                  {tableColumns.map((c) => <th key={c.name} className="px-2 py-2 text-left">{c.name}</th>)}
                </tr>
              </thead>
              <tbody>
                {(report?.rows || []).slice(0, 30).map((row, idx) => (
                  <tr key={idx} className="border-b">
                    {tableColumns.map((c) => <td key={c.name} className="px-2 py-2">{String(row[c.name] ?? "")}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
          </CardContent>
        </Card>

        {drillRows.length > 0 && (
          <Card>
            <CardHeader><CardTitle>Drill-down Result</CardTitle></CardHeader>
            <CardContent className="max-h-80 overflow-auto">
              <pre className="text-xs">{JSON.stringify(drillRows, null, 2)}</pre>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}
