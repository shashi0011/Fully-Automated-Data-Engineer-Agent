import { NextResponse } from 'next/server';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function GET() {
  try {
    const response = await fetch(`${BACKEND_URL}/dashboard/charts`, {
      signal: AbortSignal.timeout(15000),
    });
    const text = await response.text();
    let data: any;
    try {
      data = JSON.parse(text);
    } catch {
      console.error('Non-JSON response from dashboard/charts:', text.slice(0, 200));
      return NextResponse.json({
        pipeline_runs: [],
        primary_chart: [],
        secondary_chart: [],
        trend_chart: []
      });
    }
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