import { NextRequest, NextResponse } from 'next/server';

const BACKEND_PORT = 3001;

export async function GET() {
  try {
    const response = await fetch(`http://localhost:${BACKEND_PORT}/pipelines?XTransformPort=${BACKEND_PORT}`);
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Pipelines API error:', error);
    return NextResponse.json(
      { pipelines: [], count: 0, error: 'Failed to fetch pipelines' },
      { status: 500 }
    );
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    
    const response = await fetch(`http://localhost:${BACKEND_PORT}/pipelines/execute?XTransformPort=${BACKEND_PORT}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Pipeline execute API error:', error);
    return NextResponse.json(
      { status: 'error', message: 'Failed to execute pipeline' },
      { status: 500 }
    );
  }
}
