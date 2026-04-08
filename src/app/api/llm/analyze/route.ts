import { NextRequest, NextResponse } from 'next/server';
import { getUserIdFromRequest } from '@/lib/user-context';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function POST(request: NextRequest) {
    try {
    const userId = getUserIdFromRequest(request);
    const body = await request.json().catch(() => ({}));
      const response = await fetch(`${BACKEND_URL}/llm/analyze`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ...body, user_id: body.user_id || userId }),
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
