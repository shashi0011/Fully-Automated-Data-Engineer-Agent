"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { 
  GitBranch, 
  Database, 
  FileText, 
  TrendingUp,
  ArrowUpRight,
  ArrowDownRight
} from "lucide-react";

interface StatsCardsProps {
  stats: {
    total_pipelines?: number;
    total_executions?: number;
    success_rate?: number;
    tables?: number;
    reports?: number;
    data_volume?: number;
  };
}

const statItems = [
  { 
    key: "total_pipelines", 
    label: "Total Pipelines", 
    icon: GitBranch, 
    color: "text-blue-500",
    bgColor: "bg-blue-500/10",
    trend: "+12%"
  },
  { 
    key: "tables", 
    label: "Tables", 
    icon: Database, 
    color: "text-green-500",
    bgColor: "bg-green-500/10",
    trend: "+5%"
  },
  { 
    key: "reports", 
    label: "Reports", 
    icon: FileText, 
    color: "text-purple-500",
    bgColor: "bg-purple-500/10",
    trend: "+8%"
  },
  { 
    key: "success_rate", 
    label: "Success Rate", 
    icon: TrendingUp, 
    color: "text-emerald-500",
    bgColor: "bg-emerald-500/10",
    trend: "+2%",
    suffix: "%"
  },
];

export function StatsCards({ stats }: StatsCardsProps) {
  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
      {statItems.map((item) => {
        const Icon = item.icon;
        const value = stats[item.key as keyof typeof stats] ?? 0;
        const trendPositive = item.trend.startsWith("+");
        
        return (
          <Card key={item.key} className="hover:shadow-md transition-shadow">
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                {item.label}
              </CardTitle>
              <div className={`p-2 rounded-lg ${item.bgColor}`}>
                <Icon className={`h-4 w-4 ${item.color}`} />
              </div>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {value.toLocaleString()}{item.suffix || ""}
              </div>
              <div className="flex items-center gap-1 mt-1">
                {trendPositive ? (
                  <ArrowUpRight className="h-4 w-4 text-green-500" />
                ) : (
                  <ArrowDownRight className="h-4 w-4 text-red-500" />
                )}
                <span className={`text-sm ${trendPositive ? "text-green-500" : "text-red-500"}`}>
                  {item.trend}
                </span>
                <span className="text-sm text-muted-foreground">from last month</span>
              </div>
            </CardContent>
          </Card>
        );
      })}
    </div>
  );
}
