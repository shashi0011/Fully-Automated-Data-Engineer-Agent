import fs from "fs";
import path from "path";

export interface UserWorkspace {
  root: string;
  rawDir: string;
  cleanDir: string;
  reportsDir: string;
  configsDir: string;
  warehouseDir: string;
  dbPath: string;
}

const SAFE_ID = /^[a-zA-Z0-9_-]{3,80}$/;

export function provisionUserWorkspace(userId: string): UserWorkspace {
  if (!SAFE_ID.test(userId)) {
    throw new Error("Invalid user id");
  }

  const baseRoot = process.env.DATAFORGE_BASE_PATH || process.cwd();
  const root = path.join(baseRoot, "tenants", userId);

  const rawDir = path.join(root, "data", "raw");
  const cleanDir = path.join(root, "data", "clean");
  const reportsDir = path.join(root, "reports");
  const configsDir = path.join(root, "configs");
  const warehouseDir = path.join(root, "warehouse");
  const dbPath = path.join(warehouseDir, "warehouse.duckdb");

  [rawDir, cleanDir, reportsDir, configsDir, warehouseDir].forEach((dir) => fs.mkdirSync(dir, { recursive: true }));

  return {
    root,
    rawDir,
    cleanDir,
    reportsDir,
    configsDir,
    warehouseDir,
    dbPath,
  };
}
