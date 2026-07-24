import { NextResponse } from 'next/server';

import { BACKEND_URL } from '@/lib/backend-url';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ sourceType: string }> }
) {
  try {
    const { sourceType } = await params;
    
    const response = await fetch(`${BACKEND_URL}/airbyte/templates/${sourceType}`);
    const text = await response.text();
    let data: any;
    try {
      data = JSON.parse(text);
    } catch {
      console.error('Non-JSON response:', text.slice(0, 200));
      return NextResponse.json({ error: 'Backend returned invalid response' }, { status: response.status || 502 });
    }
    return NextResponse.json(data);
  } catch (error) {
    console.error('Template fetch error:', error);
    return NextResponse.json({ template: {} }, { status: 500 });
  }
}
