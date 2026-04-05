import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Download } from "lucide-react";

interface DownloadsSectionProps {
  schema: any; // Adjust type as needed0
}

const DownloadsSection: React.FC<DownloadsSectionProps> = ({ schema }) => {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Download className="h-5 w-5" />
          Downloads
        </CardTitle>
        <CardDescription>
          Download your processed data, reports, or schema information.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {schema ? (
          <div className="space-y-2">
            <Button variant="outline" className="w-full">
              <Download className="h-4 w-4 mr-2" />
              Download Schema as JSON
            </Button>
            <Button variant="outline" className="w-full">
              <Download className="h-4 w-4 mr-2" />
              Download Clean Data as CSV
            </Button>
          </div>
        ) : (
          <p className="text-muted-foreground">No data available for download. Upload a dataset first.</p>
        )}
      </CardContent>
    </Card>
  );
};

export default DownloadsSection;