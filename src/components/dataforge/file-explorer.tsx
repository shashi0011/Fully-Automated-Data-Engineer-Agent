"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { 
  Folder, 
  File, 
  ChevronRight, 
  ChevronDown,
  FileCode,
  FileSpreadsheet,
  Database,
  Download,
  Eye,
  Trash2,
  RefreshCw,
  Loader2
} from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

interface FileItem {
  name: string;
  path: string;
  type: string;
  category: string;
  size: number;
  content?: string;
  modified?: string;
}

interface FileExplorerProps {
  files?: FileItem[];
  onRefresh?: () => void;
  filterCategory?: string;
  onSelectFile?: (file: FileItem) => void;
  activeFilePath?: string;
}

const folderStructure: Record<string, { icon: typeof FileCode; color: string }> = {
  "pipelines/": { icon: FileCode, color: "text-blue-500" },
  "data/raw/": { icon: FileSpreadsheet, color: "text-green-500" },
  "data/clean/": { icon: FileSpreadsheet, color: "text-emerald-500" },
  "reports/": { icon: FileSpreadsheet, color: "text-purple-500" },
  "warehouse/": { icon: Database, color: "text-orange-500" },
};

export function FileExplorer({ files: externalFiles, onRefresh: externalOnRefresh, filterCategory, onSelectFile, activeFilePath }: FileExplorerProps) {  const [internalFiles, setInternalFiles] = useState<FileItem[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [expandedFolders, setExpandedFolders] = useState<Set<string>>(new Set(["data/raw/", "data/clean/", "pipelines/", "reports/"]));
  const [selectedFile, setSelectedFile] = useState<FileItem | null>(null);
  const [viewDialogOpen, setViewDialogOpen] = useState(false);
  const [viewLoading, setViewLoading] = useState(false);
  const [viewContent, setViewContent] = useState<string[][] | null>(null);
  const [viewRawContent, setViewRawContent] = useState<string | null>(null);

  // Determine which files to use: external props or internal state
  const files = externalFiles ?? internalFiles;

  // Fetch files internally if no external files provided
  useEffect(() => {
    if (externalFiles === undefined) {
      fetchFiles();
    }
  }, [externalFiles]);

  const fetchFiles = async () => {
    setIsLoading(true);
    try {
      const response = await fetch("/api/files");
      const data = await response.json();
      let fetchedFiles: FileItem[] = data.files || [];
      if (filterCategory) {
        fetchedFiles = fetchedFiles.filter((f: FileItem) => f.category === filterCategory);
      }
      setInternalFiles(fetchedFiles);
    } catch (error) {
      console.error("Failed to fetch files:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleRefresh = () => {
    if (externalOnRefresh) {
      externalOnRefresh();
    } else {
      fetchFiles();
    }
  };

  const toggleFolder = (folder: string) => {
    const newExpanded = new Set(expandedFolders);
    if (newExpanded.has(folder)) {
      newExpanded.delete(folder);
    } else {
      newExpanded.add(folder);
    }
    setExpandedFolders(newExpanded);
  };

  // ✅ FIX: Fetch file content from API, parse CSV into table rows
  const handleViewFile = async (file: FileItem) => {
    setSelectedFile(file);
    setViewDialogOpen(true);
    setViewLoading(true);
    setViewContent(null);
    setViewRawContent(null);

    try {
      
      const response = await fetch(`/api/files?path=${encodeURIComponent(file.path)}`);
      const data = await response.json();

      if (data.content) {
        // Parse CSV content into rows
        if (file.type === "csv") {
          const lines = data.content.split("\n").filter((l: string) => l.trim());
          const parsed = lines.map((line: string) => {
            const row: string[] = [];
            let current = "";
            let inQuotes = false;
            for (let i = 0; i < line.length; i++) {
              const char = line[i];
              if (char === '"') {
                inQuotes = !inQuotes;
              } else if (char === "," && !inQuotes) {
                row.push(current.trim());
                current = "";
              } else {
                current += char;
              }
            }
            row.push(current.trim());
            return row;
          });
          setViewContent(parsed);
        } else if (file.type === "json") {
          // Pretty-print JSON
          try {
            const parsed = JSON.parse(data.content);
            setViewRawContent(JSON.stringify(parsed, null, 2));
          } catch {
            setViewRawContent(data.content);
          }
        } else {
          setViewRawContent(data.content);
        }
      } else {
        setViewRawContent("No content available for this file.");
      }
    } catch (error) {
      console.error("Failed to fetch file content:", error);
      setViewRawContent("Failed to load file content. The backend may not be running.");
    } finally {
      setViewLoading(false);
    }
  };

  const handleDownloadFile = async (file: FileItem) => {
    try {
      const response = await fetch(`/api/download?path=${encodeURIComponent(file.path)}`);
      if (!response.ok) {
        console.error("Download failed:", response.status);
        return;
      }
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = file.name;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error("Download failed:", error);
    }
  };

  const formatSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  // Group files by folder — ✅ FIX: only group files that exist in folderStructure folders
  const filesByFolder: Record<string, FileItem[]> = {};
  files.forEach((file) => {
    const folderPath = file.path.replace(file.name, "");
    // Only include folders defined in folderStructure (skips stray/orphan files)
    if (folderStructure[folderPath]) {
      if (!filesByFolder[folderPath]) {
        filesByFolder[folderPath] = [];
      }
      filesByFolder[folderPath].push(file);
    }
  });

  // Get all unique folders from folderStructure
  const allFolders = Object.keys(folderStructure);

  return (
    <Card className="h-full">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg">
            {filterCategory ? `Reports` : "File Explorer"}
          </CardTitle>
          <div className="flex items-center gap-2">
            <Badge variant="secondary">{files.length} files</Badge>
            <Button variant="ghost" size="icon" className="h-8 w-8" onClick={handleRefresh} disabled={isLoading}>
              {isLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            </Button>
          </div>
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <ScrollArea className="h-[500px]">
          <div className="p-4 space-y-1">
            {isLoading && files.length === 0 ? (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground mr-2" />
                <span className="text-muted-foreground">Loading files...</span>
              </div>
            ) : files.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-12">
                <Folder className="h-12 w-12 text-muted-foreground mb-4" />
                <p className="text-muted-foreground text-center">
                  {filterCategory ? "No reports found" : "No files found"}
                </p>
                <Button variant="outline" size="sm" className="mt-4" onClick={handleRefresh}>
                  <RefreshCw className="h-4 w-4 mr-1" />
                  Refresh
                </Button>
              </div>
            ) : (
              allFolders.map((folder) => {
                const config = folderStructure[folder];
                if (!config) return null;
                const isExpanded = expandedFolders.has(folder);
                const folderFiles = filesByFolder[folder] || [];

                // ✅ FIX: Don't render empty folders
                if (folderFiles.length === 0) return null;

                return (
                  <div key={folder}>
                    {/* Folder Header */}
                    <button
                      onClick={() => toggleFolder(folder)}
                      className="flex items-center gap-2 w-full py-2 px-2 hover:bg-muted rounded-lg transition-colors"
                    >
                      {isExpanded ? (
                        <ChevronDown className="h-4 w-4" />
                      ) : (
                        <ChevronRight className="h-4 w-4" />
                      )}
                      <config.icon className={`h-4 w-4 ${config.color}`} />
                      <span className="font-medium">{folder}</span>
                      <Badge variant="outline" className="ml-auto">
                        {folderFiles.length}
                      </Badge>
                    </button>

                    {/* Folder Contents */}
                    {isExpanded && folderFiles.length > 0 && (
                                            <div className="ml-6 mt-1 space-y-1">
                        {folderFiles.map((file) => (
                          <div
                            key={file.path}
                            className={`flex items-center gap-2 py-1.5 px-2 hover:bg-muted rounded-lg transition-colors group cursor-pointer ${activeFilePath === file.path ? "bg-primary/10 border border-primary/30 rounded-lg" : ""}`}
                            onClick={() => { if (onSelectFile) onSelectFile(file); }}
                          >
                            <File className={`h-4 w-4 ${activeFilePath === file.path ? "text-primary" : "text-muted-foreground"}`} />
                            <span className={`flex-1 text-sm truncate ${activeFilePath === file.path ? "font-semibold text-primary" : ""}`}>{file.name}</span>
                            <span className="text-xs text-muted-foreground">
                              {formatSize(file.size)}
                            </span>
                            <div className="hidden group-hover:flex items-center gap-1">
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-6 w-6"
                                onClick={() => handleViewFile(file)}
                                title="Preview"
                              >
                                <Eye className="h-3 w-3" />
                              </Button>
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-6 w-6"
                                onClick={() => handleDownloadFile(file)}
                                title="Download"
                              >
                                <Download className="h-3 w-3" />
                              </Button>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                );
              })
            )}
          </div>
        </ScrollArea>
      </CardContent>

      {/* ✅ FIX: File Preview Dialog with proper content loading */}
      <Dialog open={viewDialogOpen} onOpenChange={setViewDialogOpen}>
        <DialogContent className="max-w-4xl max-h-[80vh]">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <File className="h-5 w-5" />
              {selectedFile?.name}
            </DialogTitle>
            <DialogDescription>
              {selectedFile && `${formatSize(selectedFile.size)} · ${selectedFile.path}`}
            </DialogDescription>
          </DialogHeader>

          {viewLoading ? (
            <div className="flex items-center justify-center py-16">
              <Loader2 className="h-8 w-8 animate-spin text-primary mr-3" />
              <span className="text-muted-foreground">Loading preview...</span>
            </div>
          ) : viewContent && viewContent.length > 0 && selectedFile?.type === "csv" ? (
            /* ✅ CSV Table Preview */
            <ScrollArea className="h-[60vh]">
              <div className="rounded-lg border overflow-hidden">
                <Table>
                  <TableHeader>
                    <TableRow>
                      {viewContent[0]?.map((header, i) => (
                        <TableHead key={i} className="text-xs font-semibold whitespace-nowrap bg-muted/50">
                          {header}
                        </TableHead>
                      ))}
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {viewContent.slice(1, 101).map((row, i) => (
                      <TableRow key={i}>
                        {row.map((cell, j) => (
                          <TableCell key={j} className="text-xs whitespace-nowrap max-w-[250px] truncate">
                            {cell}
                          </TableCell>
                        ))}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
              <p className="text-[10px] text-muted-foreground mt-2 text-right">
                Showing {Math.min(viewContent.length - 1, 100)} of {viewContent.length - 1} rows
              </p>
            </ScrollArea>
          ) : viewRawContent ? (
            /* Raw content (JSON, SQL, etc.) */
            <ScrollArea className="h-[60vh]">
              <pre className="p-4 bg-muted rounded-lg text-xs overflow-x-auto whitespace-pre-wrap">
                {viewRawContent}
              </pre>
            </ScrollArea>
          ) : (
            <div className="flex flex-col items-center justify-center py-16">
              <File className="h-12 w-12 text-muted-foreground mb-4" />
              <p className="text-muted-foreground">No content available for this file.</p>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </Card>
  );
}