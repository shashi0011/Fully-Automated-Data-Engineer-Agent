import { NextRequest, NextResponse } from 'next/server';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function GET() {
  try {
    const response = await fetch(`${BACKEND_URL}/pipelines`, {
      signal: AbortSignal.timeout(15000),
    });
    const text = await response.text();
    let data: any;
    try {
      data = JSON.parse(text);
    } catch {
      console.error('Non-JSON response from /pipelines:', text.slice(0, 200));
      return NextResponse.json({ pipelines: [], count: 0, error: 'Backend returned invalid response' }, { status: response.status || 502 });
    }
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
    let body = {};
    try {
      const rawBody = await request.text();
      if (rawBody.trim()) {
        body = JSON.parse(rawBody);
      }
    } catch {
      body = {};
    }

    const response = await fetch(`${BACKEND_URL}/pipelines/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(30000),
    });

    const text = await response.text();
    let data: any;
    try {
      data = JSON.parse(text);
    } catch {
      console.error('Non-JSON response from /pipelines/execute:', text.slice(0, 200));
      return NextResponse.json({ status: 'error', message: 'Backend returned invalid response' }, { status: response.status || 502 });
    }
    return NextResponse.json(data);
  } catch (error) {
    console.error('Pipeline execute API error:', error);
    return NextResponse.json(
      { status: 'error', message: 'Failed to execute pipeline' },
      { status: 500 }
    );
  }
}