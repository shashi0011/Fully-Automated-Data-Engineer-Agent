import { NextRequest, NextResponse } from 'next/server';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function GET() {
  try {
    const response = await fetch(`${BACKEND_URL}/schema`);
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Schema fetch error:', error);
    return NextResponse.json({ schema: null }, { status: 500 });
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const response = await fetch(`${BACKEND_URL}/schema/detect?file_path=${encodeURIComponent(body.file_path)}`, {
      method: 'POST'
    });
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Schema detect error:', error);
    return NextResponse.json({ error: 'Failed to detect schema' }, { status: 500 });
  }
}
