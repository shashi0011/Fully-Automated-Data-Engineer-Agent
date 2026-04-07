import { NextRequest, NextResponse } from 'next/server';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const response = await fetch(`${BACKEND_URL}/active-file`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    const text = await response.text();
    let data: any;
    try {
      data = JSON.parse(text);
    } catch {
      return NextResponse.json({ status: 'error', message: 'Backend returned invalid response' }, { status: response.status || 502 });
    }
    return NextResponse.json(data, { status: response.status });
  } catch (error) {
    console.error('Active file API error:', error);
    return NextResponse.json(
      { status: 'error', message: 'Failed to set active file' },
      { status: 500 }
    );
  }
}
