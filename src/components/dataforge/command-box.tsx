"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { 
  Play, 
  Loader2, 
  Sparkles, 
  GitBranch, 
  Database, 
  FileText,
  BarChart3 
} from "lucide-react";

interface CommandBoxProps {
  onExecute: (command: string) => Promise<void>;
  isLoading?: boolean;
  suggestedCommands?: string[];
}

const defaultSuggestions = [
  { icon: GitBranch, text: "Create pipeline from sales.csv", color: "text-blue-500" },
  { icon: Database, text: "Clean and transform data", color: "text-green-500" },
  { icon: BarChart3, text: "Generate sales report", color: "text-purple-500" },
  { icon: FileText, text: "Show data summary", color: "text-orange-500" },
];

export function CommandBox({ 
  onExecute, 
  isLoading = false,
  suggestedCommands 
}: CommandBoxProps) {
  const [command, setCommand] = useState("");

  const handleSubmit = async () => {
    if (command.trim()) {
      await onExecute(command.trim());
    }
  };

  const handleSuggestionClick = (suggestion: string) => {
    setCommand(suggestion);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
      handleSubmit();
    }
  };

  const suggestions = suggestedCommands 
    ? defaultSuggestions.map((s, i) => ({ ...s, text: suggestedCommands[i] || s.text }))
    : defaultSuggestions;

  return (
    <Card className="w-full shadow-lg border-2 border-primary/10">
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-xl">
          <Sparkles className="h-5 w-5 text-violet-500" />
          Tell Omnix Agent what you want to do
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="relative">
          <Textarea
            value={command}
            onChange={(e) => setCommand(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Example: Load sales.csv, clean data, and generate report"
            className="min-h-[120px] resize-none text-base pr-12"
            disabled={isLoading}
          />
          <div className="absolute bottom-2 right-2 text-xs text-muted-foreground">
            ⌘ + Enter to run
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          <span className="text-sm text-muted-foreground">Try:</span>
          {suggestions.map((suggestion, index) => (
            <Badge
              key={index}
              variant="secondary"
              className="cursor-pointer hover:bg-secondary/80 transition-colors"
              onClick={() => handleSuggestionClick(suggestion.text)}
            >
              <suggestion.icon className={`h-3 w-3 mr-1 ${suggestion.color}`} />
              {suggestion.text}
            </Badge>
          ))}
        </div>

        <Button
          onClick={handleSubmit}
          disabled={!command.trim() || isLoading}
          className="w-full bg-gradient-to-r from-violet-600 to-cyan-500 hover:from-violet-700 hover:to-cyan-600 h-11"
        >
          {isLoading ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Executing...
            </>
          ) : (
            <>
              <Play className="mr-2 h-4 w-4" />
              Run Command
            </>
          )}
        </Button>
      </CardContent>
    </Card>
  );
}
