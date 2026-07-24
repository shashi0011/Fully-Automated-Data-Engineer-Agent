import { NextRequest, NextResponse } from 'next/server';
import { getUserIdFromRequest } from '@/lib/user-context';

import { BACKEND_URL } from '@/lib/backend-url';

export async function GET(request: NextRequest) {
  try {
    const userId = getUserIdFromRequest(request);
    const { searchParams } = new URL(request.url);
    const params = new URLSearchParams();
    if (userId) params.set('user_id', userId);
    ['group_by', 'group_value', 'limit'].forEach((key) => {
      const val = searchParams.get(key);
      if (val) params.set(key, val);
    });

    const response = await fetch(`${BACKEND_URL}/report/drilldown?${params.toString()}`);
    const text = await response.text();
    let data: any;
    try {
      data = JSON.parse(text);
    } catch {
      return NextResponse.json({ status: 'error', message: 'Backend returned invalid response' }, { status: response.status || 502 });
    }
    return NextResponse.json(data, { status: response.status });
  } catch (error) {
    console.error('Drilldown API error:', error);
    return NextResponse.json({ status: 'error', message: 'Failed to drill down report' }, { status: 500 });
  }
}
