"use client";

import { useEffect, useState } from "react";

/**
 * Fires once when the frontend mounts. Calls /api/wake, which pings the
 * backend's /health endpoint and retries with backoff until it responds.
 * This is what makes "just open the frontend" also wake up the backend,
 * instead of needing to separately visit the backend URL first.
 */
export function WakeBackend() {
  const [status, setStatus] = useState<"waking" | "awake" | "unreachable" | null>(null);

  useEffect(() => {
    let cancelled = false;
    setStatus("waking");

    fetch("/api/wake")
      .then((res) => res.json())
      .then((data) => {
        if (!cancelled) setStatus(data.status === "awake" ? "awake" : "unreachable");
      })
      .catch(() => {
        if (!cancelled) setStatus("unreachable");
      });

    return () => {
      cancelled = true;
    };
  }, []);

  if (status === "waking") {
    return (
      <div className="fixed bottom-4 right-4 z-50 rounded-md border bg-card px-3 py-2 text-xs text-muted-foreground shadow-md">
        Waking up backend service&hellip; this can take up to a minute on the free tier.
      </div>
    );
  }

  if (status === "unreachable") {
    return (
      <div className="fixed bottom-4 right-4 z-50 rounded-md border bg-card px-3 py-2 text-xs text-destructive shadow-md">
        Backend isn&apos;t responding yet. Try refreshing in a moment.
      </div>
    );
  }

  return null;
}
