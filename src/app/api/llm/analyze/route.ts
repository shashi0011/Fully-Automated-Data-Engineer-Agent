import { NextRequest, NextResponse } from 'next/server';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function POST() {
  try {
    const response = await fetch(`${BACKEND_URL}/llm/analyze`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    
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
    console.error('LLM analyze error:', error);
    return NextResponse.json(
      { status: 'error', message: 'Failed to analyze dataset' },
      { status: 500 }
    );
  }
}
