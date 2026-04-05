import { NextRequest, NextResponse } from 'next/server';

const BACKEND_PORT = 3001;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const tableName = searchParams.get('table');

    if (tableName) {
      const limit = searchParams.get('limit') || '100';
      const response = await fetch(`${BACKEND_URL}/warehouse/tables/${tableName}?limit=${limit}`, {
        signal: AbortSignal.timeout(15000),
      });
      const text = await response.text();
      let data: any;
      try {
        data = JSON.parse(text);
      } catch {
        console.error('Non-JSON response from warehouse table:', text.slice(0, 200));
        return NextResponse.json({ error: 'Backend returned invalid response' }, { status: response.status || 502 });
      }
      return NextResponse.json(data);
    }

    const response = await fetch(`${BACKEND_URL}/warehouse/tables`, {
      signal: AbortSignal.timeout(15000),
    });
    const text = await response.text();
    let data: any;
    try {
      data = JSON.parse(text);
    } catch {
      console.error('Non-JSON response from warehouse tables:', text.slice(0, 200));
      return NextResponse.json({ tables: [], count: 0, error: 'Backend returned invalid response' }, { status: response.status || 502 });
    }
    return NextResponse.json(data);
  } catch (error) {
    console.error('Warehouse API error:', error);
    return NextResponse.json(
      { tables: [], count: 0, error: 'Failed to fetch warehouse data' },
      { status: 500 }
    );
  }
}