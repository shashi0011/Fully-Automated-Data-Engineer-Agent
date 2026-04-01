import { NextRequest, NextResponse } from 'next/server';

const BACKEND_PORT = 3001;

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const tableName = searchParams.get('table');
    
    if (tableName) {
      const limit = searchParams.get('limit') || '100';
      const response = await fetch(`http://localhost:${BACKEND_PORT}/warehouse/tables/${tableName}?limit=${limit}&XTransformPort=${BACKEND_PORT}`);
      const data = await response.json();
      return NextResponse.json(data);
    }
    
    const response = await fetch(`http://localhost:${BACKEND_PORT}/warehouse/tables?XTransformPort=${BACKEND_PORT}`);
    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Warehouse API error:', error);
    return NextResponse.json(
      { tables: [], count: 0, error: 'Failed to fetch warehouse data' },
      { status: 500 }
    );
  }
}
