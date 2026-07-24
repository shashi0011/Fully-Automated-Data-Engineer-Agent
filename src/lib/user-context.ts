import { NextRequest } from 'next/server';

export function getUserIdFromRequest(request: NextRequest): string | null {
  const fromCookie = request.cookies.get('df_user_id')?.value;
  if (fromCookie && fromCookie.trim()) return fromCookie;

  const fromHeader = request.headers.get('x-user-id');
  if (fromHeader && fromHeader.trim()) return fromHeader;

  const fromQuery = new URL(request.url).searchParams.get('user_id');
  if (fromQuery && fromQuery.trim()) return fromQuery;

  return null;
}
