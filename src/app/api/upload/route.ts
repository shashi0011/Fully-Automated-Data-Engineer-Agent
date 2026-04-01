import { NextRequest, NextResponse } from 'next/server';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function POST(request: NextRequest) {
  try {
    const formData = await request.formData();
    
    const response = await fetch(`${BACKEND_URL}/upload-and-process`, {
      method: 'POST',
      body: formData
    });
    
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Upload error:', error);
    return NextResponse.json({ error: 'Failed to upload file' }, { status: 500 });
  }
}

export async function GET() {
  try {
    const response = await fetch(`${BACKEND_URL}/files`);
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Files fetch error:', error);
    return NextResponse.json({ files: [] }, { status: 500 });
  }
}
