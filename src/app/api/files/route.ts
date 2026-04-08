import { NextRequest, NextResponse } from 'next/server';
import { getUserIdFromRequest } from '@/lib/user-context';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function GET(request: NextRequest) {
  try {
    const userId = getUserIdFromRequest(request);
    const { searchParams } = new URL(request.url);
    const filePath = searchParams.get('path');

    if (filePath) {
      const suffix = userId ? `?user_id=${encodeURIComponent(userId)}` : '';
      const response = await fetch(`${BACKEND_URL}/files/${encodeURIComponent(filePath)}${suffix}`);
      const text = await response.text();
      let data: any;
      try {
        data = JSON.parse(text);
      } catch {
        console.error('Non-JSON response:', text.slice(0, 200));
        return NextResponse.json({ error: 'Backend returned invalid response' }, { status: response.status || 502 });
      }
      return NextResponse.json(data);
    }

    const suffix = userId ? `?user_id=${encodeURIComponent(userId)}` : '';
    const response = await fetch(`${BACKEND_URL}/files${suffix}`);
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
    console.error('Files API error:', error);
    return NextResponse.json(
      { files: [], count: 0, error: 'Failed to fetch files' },
      { status: 500 }
    );
  }
}
