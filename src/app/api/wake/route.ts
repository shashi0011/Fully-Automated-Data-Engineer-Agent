import { NextResponse } from "next/server";
import { BACKEND_URL } from "@/lib/backend-url";

// Render's free tier spins down services after ~15 minutes idle.
// Visiting the frontend does NOT wake the backend on its own — they're
// two independent services. This route is called once when the frontend
// loads (see WakeBackend component) to explicitly ping the backend and
// wait for it to respond, retrying with backoff since a cold start can
// take 30-60 seconds.
export async function GET() {
  const maxAttempts = 8;
  const delaysMs = [500, 1000, 2000, 3000, 5000, 8000, 8000, 8000];

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000);
      const res = await fetch(`${BACKEND_URL}/health`, {
        signal: controller.signal,
        cache: "no-store",
      });
      clearTimeout(timeout);

      if (res.ok) {
        return NextResponse.json({ status: "awake", attempt: attempt + 1 });
      }
    } catch {
      // backend still asleep / cold-starting — fall through and retry
    }

    if (attempt < maxAttempts - 1) {
      await new Promise((resolve) => setTimeout(resolve, delaysMs[attempt]));
    }
  }

  return NextResponse.json(
    { status: "unreachable", message: "Backend did not respond after retries." },
    { status: 503 }
  );
}
