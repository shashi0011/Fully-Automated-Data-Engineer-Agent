"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { 
  GitBranch, 
  Play, 
  ArrowRight, 
  Database, 
  FileText, 
  BarChart3,
  CheckCircle2,
  Circle,
  Loader2
} from "lucide-react";

interface PipelineDAGProps {
  status?: "idle" | "running" | "success" | "error";
  onRun?: () => void;
  isRunning?: boolean;
}

const stages = [
  { id: "source", name: "Data Source", icon: Database, description: "Raw sales.csv" },
  { id: "ingest", name: "Ingest", icon: FileText, description: "Load to warehouse" },
  { id: "transform", name: "Transform", icon: GitBranch, description: "Clean & process" },
  { id: "warehouse", name: "Warehouse", icon: Database, description: "DuckDB storage" },
  { id: "report", name: "Report", icon: BarChart3, description: "Generate output" },
];

export function PipelineDAG({ status = "idle", onRun, isRunning = false }: PipelineDAGProps) {
  const getStageStatus = (index: number) => {
    if (status === "running") return "pending";
    if (status === "success") return "success";
    if (status === "error") return index < 2 ? "success" : "error";
    return "pending";
  };

  return (
    <Card className="shadow-lg">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <GitBranch className="h-5 w-5 text-violet-500" />
            Pipeline DAG
          </CardTitle>
          <div className="flex items-center gap-2">
            <Badge
              variant={status === "success" ? "default" : status === "error" ? "destructive" : "secondary"}
            >
              {status}
            </Badge>
            {onRun && (
              <Button
                onClick={onRun}
                disabled={isRunning}
                size="sm"
                className="bg-gradient-to-r from-violet-600 to-cyan-500"
              >
                {isRunning ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                    Running
                  </>
                ) : (
                  <>
                    <Play className="h-4 w-4 mr-1" />
                    Run Pipeline
                  </>
                )}
              </Button>
            )}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <div className="flex items-center justify-between gap-2 overflow-x-auto pb-4">
          {stages.map((stage, index) => {
            const stageStatus = getStageStatus(index);
            const Icon = stage.icon;
            
            return (
              <div key={stage.id} className="flex items-center gap-2">
                {/* Stage Node */}
                <div
                  className={`
                    flex flex-col items-center p-4 rounded-xl border-2 min-w-[120px] transition-all
                    ${stageStatus === "success" 
                      ? "border-green-500 bg-green-500/10" 
                      : stageStatus === "error"
                      ? "border-red-500 bg-red-500/10"
                      : "border-muted bg-muted/50"
                    }
                  `}
                >
                  <div className={`
                    w-12 h-12 rounded-full flex items-center justify-center mb-2
                    ${stageStatus === "success" 
                      ? "bg-green-500 text-white" 
                      : stageStatus === "error"
                      ? "bg-red-500 text-white"
                      : "bg-muted text-muted-foreground"
                    }
                  `}>
                    {stageStatus === "success" ? (
                      <CheckCircle2 className="h-6 w-6" />
                    ) : (
                      <Icon className="h-6 w-6" />
                    )}
                  </div>
                  <span className="font-medium text-sm">{stage.name}</span>
                  <span className="text-xs text-muted-foreground text-center mt-1">
                    {stage.description}
                  </span>
                </div>

                {/* Arrow */}
                {index < stages.length - 1 && (
                  <ArrowRight className="h-5 w-5 text-muted-foreground shrink-0" />
                )}
              </div>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}
