import { NextRequest, NextResponse } from 'next/server';
import { getUserIdFromRequest } from '@/lib/user-context';

import { BACKEND_URL } from '@/lib/backend-url';

export async function GET(request: NextRequest) {
  try {
    const userId = getUserIdFromRequest(request);
    const suffix = userId ? `?user_id=${encodeURIComponent(userId)}` : '';
    const response = await fetch(`${BACKEND_URL}/dashboard/stats${suffix}`);
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
    console.error('Stats API error:', error);
    return NextResponse.json({
      total_pipelines: 0,
      total_executions: 0,
      success_rate: 0,
      tables: 0,
      reports: 0,
      data_volume: 0,
      dataset_type: 'none',
      current_table: 'none',
    });
  }
}
