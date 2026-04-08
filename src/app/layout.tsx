import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import { Toaster } from "@/components/ui/toaster";
import { Providers } from "@/components/providers";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Omnix - AI-Powered Data Pipeline Platform",
  description: "Build data pipelines with AI. Upload any dataset, get LLM-powered analysis, generate dbt models, and create production-ready pipelines.",
  keywords: ["Omnix", "AI", "data pipeline", "dbt", "LLM", "DuckDB"],
  authors: [{ name: "Omnix AI" }],
  icons: {
    icon: "/assets/logo.png",
  },
  openGraph: {
    title: "Omnix AI",
    description: "Build data pipelines with AI — No coding required",
    type: "website",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased bg-background text-foreground`}
      >
        <Providers>
          {children}
          <Toaster />
        </Providers>
      </body>
    </html>
  );
}
