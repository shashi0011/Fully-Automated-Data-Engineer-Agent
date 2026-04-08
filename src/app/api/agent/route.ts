import { NextRequest, NextResponse } from 'next/server';
import { getUserIdFromRequest } from '@/lib/user-context';

import { BACKEND_URL } from '@/lib/backend-url';

export async function POST(request: NextRequest) {
  try {
    const userId = getUserIdFromRequest(request);
    const body = await request.json();

    const response = await fetch(`${BACKEND_URL}/run-agent`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
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
    console.error('Agent API error:', error);
    return NextResponse.json(
      { status: 'error', message: 'Failed to execute agent command' },
      { status: 500 }
    );
  }
}

export async function GET(request: NextRequest) {
  try {
    const userId = getUserIdFromRequest(request);
    const suffix = userId ? `?user_id=${encodeURIComponent(userId)}` : '';
    const response = await fetch(`${BACKEND_URL}/status${suffix}`);
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
    console.error('Status API error:', error);
    return NextResponse.json(
      { status: 'error', message: 'Failed to get status' },
      { status: 500 }
    );
  }
}
