import { NextRequest, NextResponse } from 'next/server';
import { getUserIdFromRequest } from '@/lib/user-context';

import { BACKEND_URL } from '@/lib/backend-url';

export async function POST(request: NextRequest) {
  try {
    const userId = getUserIdFromRequest(request);
    const formData = await request.formData();
    if (userId) {
      formData.set('user_id', userId);
    }

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

export async function GET(request: NextRequest) {
  try {
    const userId = getUserIdFromRequest(request);
    const suffix = userId ? `?user_id=${encodeURIComponent(userId)}` : '';
    const response = await fetch(`${BACKEND_URL}/files${suffix}`);
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
