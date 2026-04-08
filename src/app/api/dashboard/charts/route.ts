import { NextRequest, NextResponse } from 'next/server';
import { getUserIdFromRequest } from '@/lib/user-context';

import { BACKEND_URL } from '@/lib/backend-url';

export async function GET(request: NextRequest) {
  try {
    const userId = getUserIdFromRequest(request);
    const suffix = userId ? `?user_id=${encodeURIComponent(userId)}` : '';
    const response = await fetch(`${BACKEND_URL}/dashboard/charts${suffix}`, {
      signal: AbortSignal.timeout(15000),
    });
    const text = await response.text();
    let data: any;
    try {
      data = JSON.parse(text);
    } catch {
      console.error('Non-JSON response from dashboard/charts:', text.slice(0, 200));
      return NextResponse.json({
        pipeline_runs: [],
        primary_chart: [],
        secondary_chart: [],
        trend_chart: []
      });
    }
    return NextResponse.json(data);
  } catch (error) {
    console.error('Charts API error:', error);
    return NextResponse.json({
      pipeline_runs: [],
      primary_chart: [],
      secondary_chart: [],
      trend_chart: []
    });
  }
}
