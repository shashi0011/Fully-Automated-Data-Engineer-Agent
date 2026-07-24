import { NextRequest, NextResponse } from 'next/server';
import { getUserIdFromRequest } from '@/lib/user-context';

import { BACKEND_URL } from '@/lib/backend-url';

export async function POST(request: NextRequest) {
  try {
    const userId = getUserIdFromRequest(request);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 120000);
    const body = await request.json().catch(() => ({}));
    try {
      const response = await fetch(`${BACKEND_URL}/llm/generate-dbt`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...body, user_id: body.user_id || userId }),
      });

      const text = await response.text();

      if (!text.trim()) {
        console.error('Empty response from /llm/generate-dbt, status:', response.status);
        return NextResponse.json(
          { status: 'error', message: 'Backend returned empty response', models: [] },
          { status: response.status || 502 }
        );
      }

      let data: any;
      try {
        data = JSON.parse(text);
      } catch {
        console.error('Non-JSON response from /llm/generate-dbt:', text.slice(0, 200));
        return NextResponse.json(
          { status: 'error', message: 'Backend returned invalid response', models: [] },
          { status: response.status || 502 }
        );
      }
      return NextResponse.json(data);
    } finally {
      clearTimeout(timeout);
    }
  } catch (error: any) {
    if (error?.name === 'AbortError') {
      console.error('Generate dbt error: Backend request timed out (120s)');
      return NextResponse.json(
        { status: 'error', message: 'Request timed out - LLM is taking too long', models: [] },
        { status: 504 }
      );
    }
    console.error('Generate dbt error:', error);
    return NextResponse.json(
      { status: 'error', message: 'Failed to generate dbt models. Is the Python backend running on port 3001?', models: [] },
      { status: 500 }
    );
  }
}
