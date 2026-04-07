"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { 
  FileCode, 
  FileSpreadsheet, 
  Database, 
  Download, 
  Eye, 
  Trash2,
  CheckCircle2,
  Clock,
  XCircle
} from "lucide-react";

interface FileCardProps {
  file: {
    name: string;
    path: string;
    type: string;
    category: string;
    size: number;
    content?: string;
    status?: "success" | "pending" | "error";
    modified?: string;
  };
  onView?: () => void;
  onDownload?: () => void;
  onDelete?: () => void;
}

const fileTypeIcons: Record<string, typeof FileCode> = {
  py: FileCode,
  csv: FileSpreadsheet,
  json: FileSpreadsheet,
  xlsx: FileSpreadsheet,
  xls: FileSpreadsheet,
  duckdb: Database,
  sql: FileCode,
  yml: FileCode,
};

const categoryColors: Record<string, string> = {
  pipeline: "bg-blue-500/10 text-blue-500",
  raw_data: "bg-orange-500/10 text-orange-500",
  clean_data: "bg-green-500/10 text-green-500",
  report: "bg-purple-500/10 text-purple-500",
  warehouse: "bg-gray-500/10 text-gray-500",
  schema: "bg-cyan-500/10 text-cyan-500",
  dbt_model: "bg-amber-500/10 text-amber-500",
};

const statusIcons: Record<string, typeof CheckCircle2> = {
  success: CheckCircle2,
  pending: Clock,
  error: XCircle,
};

const statusColors: Record<string, string> = {
  success: "text-green-500",
  pending: "text-yellow-500",
  error: "text-red-500",
};

export function FileCard({ file, onView, onDownload, onDelete }: FileCardProps) {
  const Icon = fileTypeIcons[file.type] || FileCode;
  const StatusIcon = file.status ? statusIcons[file.status] : null;

  const formatSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  return (
    <Card className="hover:shadow-md transition-shadow">
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-3">
            <div className={`p-2 rounded-lg bg-muted ${statusColors[file.status || "success"]}`}>
              <Icon className="h-5 w-5" />
            </div>
            <div>
              <CardTitle className="text-base font-medium">{file.name}</CardTitle>
              <p className="text-sm text-muted-foreground">{file.path}</p>
            </div>
          </div>
          {StatusIcon && file.status && (
            <StatusIcon className={`h-5 w-5 ${statusColors[file.status]}`} />
          )}
        </div>
      </CardHeader>
      <CardContent>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Badge variant="secondary" className={categoryColors[file.category]}>
              {file.category}
            </Badge>
            <span className="text-sm text-muted-foreground">
              {formatSize(file.size)}
            </span>
          </div>
          
          <div className="flex items-center gap-1">
            {onView && (
              <Button variant="ghost" size="sm" onClick={onView}>
                <Eye className="h-4 w-4" />
              </Button>
            )}
            {onDownload && (
              <Button variant="ghost" size="sm" onClick={onDownload}>
                <Download className="h-4 w-4" />
              </Button>
            )}
            {onDelete && (
              <Button variant="ghost" size="sm" onClick={onDelete} className="text-red-500 hover:text-red-600">
                <Trash2 className="h-4 w-4" />
              </Button>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

export function FileCardSkeleton() {
  return (
    <Card className="animate-pulse">
      <CardHeader className="pb-3">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 bg-muted rounded-lg" />
          <div className="space-y-2">
            <div className="h-4 w-32 bg-muted rounded" />
            <div className="h-3 w-48 bg-muted rounded" />
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="h-6 w-16 bg-muted rounded" />
            <div className="h-4 w-12 bg-muted rounded" />
          </div>
          <div className="flex gap-1">
            <div className="h-8 w-8 bg-muted rounded" />
            <div className="h-8 w-8 bg-muted rounded" />
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
