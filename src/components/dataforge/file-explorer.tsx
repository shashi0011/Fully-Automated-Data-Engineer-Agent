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
} from "@/components/ui/dialog";

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
}

const folderStructure: Record<string, { icon: typeof FileCode; color: string }> = {
  "pipelines/": { icon: FileCode, color: "text-blue-500" },
  "data/raw/": { icon: FileSpreadsheet, color: "text-green-500" },
  "data/clean/": { icon: FileSpreadsheet, color: "text-emerald-500" },
  "reports/": { icon: FileSpreadsheet, color: "text-purple-500" },
  "warehouse/": { icon: Database, color: "text-orange-500" },
};

export function FileExplorer({ files: externalFiles, onRefresh: externalOnRefresh, filterCategory }: FileExplorerProps) {
  const [internalFiles, setInternalFiles] = useState<FileItem[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [expandedFolders, setExpandedFolders] = useState<Set<string>>(new Set(["pipelines/", "data/", "reports/"]));
  const [selectedFile, setSelectedFile] = useState<FileItem | null>(null);
  const [viewDialogOpen, setViewDialogOpen] = useState(false);

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

  const handleViewFile = (file: FileItem) => {
    setSelectedFile(file);
    setViewDialogOpen(true);
  };

  const handleDownloadFile = (file: FileItem) => {
    if (file.content) {
      const blob = new Blob([file.content], { type: "text/plain" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = file.name;
      a.click();
      URL.revokeObjectURL(url);
    }
  };

  const formatSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  // Group files by folder
  const filesByFolder: Record<string, FileItem[]> = {};
  files.forEach((file) => {
    const folderPath = file.path.replace(file.name, "");
    if (!filesByFolder[folderPath]) {
      filesByFolder[folderPath] = [];
    }
    filesByFolder[folderPath].push(file);
  });

  // Get all unique folders
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
                            className="flex items-center gap-2 py-1.5 px-2 hover:bg-muted rounded-lg transition-colors group"
                          >
                            <File className="h-4 w-4 text-muted-foreground" />
                            <span className="flex-1 text-sm truncate">{file.name}</span>
                            <span className="text-xs text-muted-foreground">
                              {formatSize(file.size)}
                            </span>
                            <div className="hidden group-hover:flex items-center gap-1">
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-6 w-6"
                                onClick={() => handleViewFile(file)}
                              >
                                <Eye className="h-3 w-3" />
                              </Button>
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-6 w-6"
                                onClick={() => handleDownloadFile(file)}
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

      {/* File View Dialog */}
      <Dialog open={viewDialogOpen} onOpenChange={setViewDialogOpen}>
        <DialogContent className="max-w-3xl max-h-[80vh]">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <File className="h-5 w-5" />
              {selectedFile?.name}
            </DialogTitle>
          </DialogHeader>
          <ScrollArea className="h-[60vh]">
            <pre className="p-4 bg-muted rounded-lg text-sm overflow-x-auto">
              <code>{selectedFile?.content || "Binary file - cannot display"}</code>
            </pre>
          </ScrollArea>
        </DialogContent>
      </Dialog>
    </Card>
  );
}
