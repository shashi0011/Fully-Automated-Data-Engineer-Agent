"use client";

import { ThemeProvider } from "next-themes";
import { WakeBackend } from "@/components/wake-backend";

export function Providers({ children }: { children: React.ReactNode }) {
  return (
    <ThemeProvider
      attribute="class"
      defaultTheme="system"
      enableSystem
      disableTransitionOnChange
    >
      <WakeBackend />
      {children}
    </ThemeProvider>
  );
}
