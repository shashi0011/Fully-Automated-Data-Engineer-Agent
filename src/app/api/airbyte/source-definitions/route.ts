import { NextResponse } from 'next/server';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function GET() {
  try {
    const response = await fetch(`${BACKEND_URL}/airbyte/source-definitions`);
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Source definitions error:', error);
    return NextResponse.json({ source_definitions: [] }, { status: 500 });
  }
}
