import { NextResponse } from 'next/server';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function GET() {
  try {
    const response = await fetch(`${BACKEND_URL}/airbyte/connections`);
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Connections error:', error);
    return NextResponse.json({ connections: [] }, { status: 500 });
  }
}

export async function POST(request: Request) {
  try {
    const body = await request.json();
    
    const response = await fetch(`${BACKEND_URL}/airbyte/connections`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Create connection error:', error);
    return NextResponse.json(
      { status: 'error', message: 'Failed to create connection' },
      { status: 500 }
    );
  }
}
