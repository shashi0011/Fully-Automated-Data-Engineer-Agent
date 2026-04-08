import { NextRequest, NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';
import { getUserIdFromRequest } from '@/lib/user-context';

const BASE_DIR = process.cwd();
const SAMPLES_DIR = path.join(BASE_DIR, 'data', 'samples');
const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function POST(request: NextRequest) {
  try {
    const userId = getUserIdFromRequest(request);
    const { searchParams } = new URL(request.url);
    const filename = searchParams.get('file');

    if (!filename) {
      return NextResponse.json({ error: 'No file specified' }, { status: 400 });
    }

    const sourcePath = path.join(SAMPLES_DIR, filename);
    if (!fs.existsSync(sourcePath)) {
      return NextResponse.json({ error: 'Sample file not found' }, { status: 404 });
    }

    const fileContent = fs.readFileSync(sourcePath);
    const formData = new FormData();
    const blob = new Blob([fileContent], { type: 'text/csv' });
    formData.append('file', blob, filename);
    if (userId) {
      formData.append('user_id', userId);
    }

    const uploadResponse = await fetch(`${BACKEND_URL}/upload-and-process`, {
      method: 'POST',
      body: formData
    });

    const text = await uploadResponse.text();
    let data: any;
    try {
      data = JSON.parse(text);
    } catch {
      console.error('Non-JSON response:', text.slice(0, 200));
      return NextResponse.json({ error: 'Backend returned invalid response' }, { status: uploadResponse.status || 502 });
    }

    return NextResponse.json({
      status: 'success',
      message: `Loaded sample: ${filename}`,
      ...data
    });

  } catch (error) {
    console.error('Load sample error:', error);
    return NextResponse.json({ error: 'Failed to load sample' }, { status: 500 });
  }
}
