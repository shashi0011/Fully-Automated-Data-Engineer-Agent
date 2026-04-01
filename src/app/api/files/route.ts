import { NextRequest, NextResponse } from 'next/server';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const filePath = searchParams.get('path');
    
    if (filePath) {
      const response = await fetch(`${BACKEND_URL}/files/${encodeURIComponent(filePath)}`);
      const data = await response.json();
      return NextResponse.json(data);
    }
    
    const response = await fetch(`${BACKEND_URL}/files`);
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Files API error:', error);
    return NextResponse.json(
      { files: [], count: 0, error: 'Failed to fetch files' },
      { status: 500 }
    );
  }
}
