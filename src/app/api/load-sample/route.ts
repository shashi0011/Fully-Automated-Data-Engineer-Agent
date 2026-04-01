import { NextRequest, NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';

const SAMPLES_DIR = '/home/z/my-project/data/samples';
const RAW_DIR = '/home/z/my-project/data/raw';
const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function POST(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const filename = searchParams.get('file');
    
    if (!filename) {
      return NextResponse.json({ error: 'No file specified' }, { status: 400 });
    }
    
    const sourcePath = path.join(SAMPLES_DIR, filename);
    const destPath = path.join(RAW_DIR, filename);
    
    // Check if source exists
    if (!fs.existsSync(sourcePath)) {
      return NextResponse.json({ error: 'Sample file not found' }, { status: 404 });
    }
    
    // Ensure destination directory exists
    if (!fs.existsSync(RAW_DIR)) {
      fs.mkdirSync(RAW_DIR, { recursive: true });
    }
    
    // Copy file to raw directory
    fs.copyFileSync(sourcePath, destPath);
    
    // Read file and send as form data to backend
    const fileContent = fs.readFileSync(destPath);
    const formData = new FormData();
    const blob = new Blob([fileContent], { type: 'text/csv' });
    formData.append('file', blob, filename);
    
    const uploadResponse = await fetch(`${BACKEND_URL}/upload-and-process`, {
      method: 'POST',
      body: formData
    });
    
    const data = await uploadResponse.json();
    
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
