import { NextRequest, NextResponse } from 'next/server';
import { getUserIdFromRequest } from '@/lib/user-context';

import { BACKEND_URL } from '@/lib/backend-url';

export async function GET(request: NextRequest) {
  try {
    const userId = getUserIdFromRequest(request);
    const { searchParams } = new URL(request.url);
    const tableName = searchParams.get('table');

    if (tableName) {
      const limit = searchParams.get('limit') || '100';
      const suffix = userId ? `&user_id=${encodeURIComponent(userId)}` : '';
      const response = await fetch(`${BACKEND_URL}/warehouse/tables/${tableName}?limit=${limit}${suffix}`, {
        signal: AbortSignal.timeout(15000),
      });
      const text = await response.text();
      let data: any;
      try {
        data = JSON.parse(text);
      } catch {
        console.error('Non-JSON response from warehouse table:', text.slice(0, 200));
        return NextResponse.json({ error: 'Backend returned invalid response' }, { status: response.status || 502 });
      }
      return NextResponse.json(data);
    }

    const suffix = userId ? `?user_id=${encodeURIComponent(userId)}` : '';
    const response = await fetch(`${BACKEND_URL}/warehouse/tables${suffix}`, {
      signal: AbortSignal.timeout(15000),
    });
    const text = await response.text();
    let data: any;
    try {
      data = JSON.parse(text);
    } catch {
      console.error('Non-JSON response from warehouse tables:', text.slice(0, 200));
      return NextResponse.json({ tables: [], count: 0, error: 'Backend returned invalid response' }, { status: response.status || 502 });
    }
    return NextResponse.json(data);
  } catch (error) {
    console.error('Warehouse API error:', error);
    return NextResponse.json(
      { tables: [], count: 0, error: 'Failed to fetch warehouse data' },
      { status: 500 }
    );
  }
}
