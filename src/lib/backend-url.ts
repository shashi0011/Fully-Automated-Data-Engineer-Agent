export const BACKEND_URL =
  process.env.BACKEND_URL?.replace(/\/+$/, "") || "http://localhost:3001";