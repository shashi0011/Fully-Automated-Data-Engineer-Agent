import { NextResponse } from 'next/server';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function POST() {
  try {
    const response = await fetch(`${BACKEND_URL}/llm/generate-dbt`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Generate dbt error:', error);
    return NextResponse.json(
      { status: 'error', message: 'Failed to generate dbt models' },
      { status: 500 }
    );
  }
}
