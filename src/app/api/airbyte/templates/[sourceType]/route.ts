import { NextResponse } from 'next/server';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function GET(
  request: Request,
  { params }: { params: Promise<{ sourceType: string }> }
) {
  try {
    const { sourceType } = await params;
    
    const response = await fetch(`${BACKEND_URL}/airbyte/templates/${sourceType}`);
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Template fetch error:', error);
    return NextResponse.json({ template: {} }, { status: 500 });
  }
}
