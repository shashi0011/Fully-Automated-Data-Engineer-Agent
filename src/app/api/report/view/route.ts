import { NextRequest, NextResponse } from 'next/server';
import { getUserIdFromRequest } from '@/lib/user-context';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function GET(request: NextRequest) {
  try {
    const userId = getUserIdFromRequest(request);
    const { searchParams } = new URL(request.url);
    const params = new URLSearchParams();

    if (userId) params.set('user_id', userId);
    ['limit', 'filter_column', 'filter_value', 'sort_by', 'sort_dir', 'bar_category', 'bar_metric'].forEach((key) => {
      const val = searchParams.get(key);
      if (val) params.set(key, val);
    });

    const response = await fetch(`${BACKEND_URL}/report/view?${params.toString()}`);
    const text = await response.text();
    let data: any;
    try {
      data = JSON.parse(text);
    } catch {
      return NextResponse.json({ status: 'error', message: 'Backend returned invalid response' }, { status: response.status || 502 });
    }
    return NextResponse.json(data, { status: response.status });
  } catch (error) {
    console.error('View report API error:', error);
    return NextResponse.json({ status: 'error', message: 'Failed to load report' }, { status: 500 });
  }
}
