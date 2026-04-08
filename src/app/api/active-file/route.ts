import { NextRequest, NextResponse } from 'next/server';
import { getUserIdFromRequest } from '@/lib/user-context';

import { BACKEND_URL } from '@/lib/backend-url';

export async function POST(request: NextRequest) {
  try {
    const userId = getUserIdFromRequest(request);
    const body = await request.json();
    const response = await fetch(`${BACKEND_URL}/active-file`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...body, user_id: body.user_id || userId }),
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
