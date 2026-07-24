export const BACKEND_URL =
  process.env.BACKEND_URL?.replace(/\/+$/, "") || "http://localhost:3001";

/**
 * Fetch against the backend, retrying on connection failures. Render's free
 * tier can take 30-60s to cold-start after being idle; a single fetch would
 * just fail with ECONNREFUSED/timeout during that window. This retries with
 * backoff so requests made right as the service wakes up still succeed
 * instead of surfacing a hard error to the user.
 */
export async function fetchBackend(
  path: string,
  init?: RequestInit,
  options?: { maxAttempts?: number }
): Promise<Response> {
  const maxAttempts = options?.maxAttempts ?? 5;
  const delaysMs = [1000, 2000, 4000, 8000];
  let lastError: unknown;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 25000);
      const res = await fetch(`${BACKEND_URL}${path}`, {
        ...init,
        signal: controller.signal,
      });
      clearTimeout(timeout);
      return res;
    } catch (err) {
      lastError = err;
      if (attempt < maxAttempts - 1) {
        await new Promise((resolve) => setTimeout(resolve, delaysMs[attempt] ?? 8000));
      }
    }
  }

  throw lastError;
}