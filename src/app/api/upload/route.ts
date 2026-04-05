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
    const text = await response.text();
    try {
      const data = JSON.parse(text);
      return NextResponse.json(data, { status: response.status });
    } catch {
      console.error('Upload: non-JSON response:', text.slice(0, 200));
      return NextResponse.json(
        { error: 'Backend returned an invalid response' },
        { status: response.status || 502 }
      );
    }
  } catch (error) {
    console.error('Upload error:', error);
    return NextResponse.json({ error: 'Failed to upload file' }, { status: 500 });
  }
}

export async function GET() {
  try {
    const response = await fetch(`${BACKEND_URL}/files`);
    const text = await response.text();
    try {
      const data = JSON.parse(text);
      return NextResponse.json(data);
    } catch {
      return NextResponse.json({ files: [] }, { status: 502 });
    }
  } catch (error) {
    console.error('Files fetch error:', error);
    return NextResponse.json({ files: [] }, { status: 500 });
  }
}