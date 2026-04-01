import { NextResponse } from 'next/server';

const BACKEND_PORT = 3001;

export async function GET() {
  try {
    const response = await fetch(`http://localhost:${BACKEND_PORT}/dashboard/charts?XTransformPort=${BACKEND_PORT}`);
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Charts API error:', error);
    return NextResponse.json({
      pipeline_runs: [],
      primary_chart: [],
      secondary_chart: [],
      trend_chart: []
    });
  }
}
