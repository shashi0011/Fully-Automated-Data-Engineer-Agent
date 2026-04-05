import { NextResponse } from 'next/server';

const BACKEND_PORT = 3001;

export async function GET() {
  try {
    const response = await fetch(`http://localhost:${BACKEND_PORT}/dashboard/charts?XTransformPort=${BACKEND_PORT}`);
    const text = await response.text();
    let data: any;
    try {
      data = JSON.parse(text);
    } catch {
      console.error('Non-JSON response:', text.slice(0, 200));
      return NextResponse.json({ error: 'Backend returned invalid response' }, { status: response.status || 502 });
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
