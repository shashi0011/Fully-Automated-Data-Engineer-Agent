import { NextResponse } from 'next/server';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function GET() {
  try {
    const response = await fetch(`${BACKEND_URL}/dashboard/stats`);
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Dashboard stats API error:', error);
    return NextResponse.json({
      total_pipelines: 0,
      total_executions: 0,
      success_rate: 0,
      tables: 0,
      reports: 0,
      data_volume: 0,
      dataset_type: 'none',
      current_table: 'none'
    });
  }
}
