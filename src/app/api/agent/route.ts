import { NextRequest, NextResponse } from 'next/server';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    
    const response = await fetch(`${BACKEND_URL}/run-agent`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Agent API error:', error);
    return NextResponse.json(
      { status: 'error', message: 'Failed to execute agent command' },
      { status: 500 }
    );
  }
}

export async function GET() {
  try {
    const response = await fetch(`${BACKEND_URL}/status`);
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Status API error:', error);
    return NextResponse.json(
      { status: 'error', message: 'Failed to get status' },
      { status: 500 }
    );
  }
}
