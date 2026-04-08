import { NextRequest, NextResponse } from 'next/server';
import { getUserIdFromRequest } from '@/lib/user-context';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function GET(request: NextRequest) {
  try {
    const userId = getUserIdFromRequest(request);
    const suffix = userId ? `?user_id=${encodeURIComponent(userId)}` : '';
    const response = await fetch(`${BACKEND_URL}/schema${suffix}`);
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
    console.error('Schema fetch error:', error);
    return NextResponse.json({ schema: null }, { status: 500 });
  }
}

export async function POST(request: NextRequest) {
  try {
    const userId = getUserIdFromRequest(request);
    const body = await request.json();
    const suffix = userId ? `&user_id=${encodeURIComponent(userId)}` : '';
    const response = await fetch(`${BACKEND_URL}/schema/detect?file_path=${encodeURIComponent(body.file_path)}${suffix}`, {
      method: 'POST'
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
    console.error('Schema detect error:', error);
    return NextResponse.json({ error: 'Failed to detect schema' }, { status: 500 });
  }
}
