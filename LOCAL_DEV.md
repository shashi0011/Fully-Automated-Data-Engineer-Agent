# Local Run Guide

Use this when you want to run the project directly on your machine (without Docker).

## Prerequisites
- Node.js 20+
- Python 3.11+ (3.12 recommended)
- npm installed

## 1) Create local env

Windows PowerShell:
```powershell
Copy-Item .env.local.example .env.local
```

Linux/macOS:
```bash
cp .env.local.example .env.local
```

Edit `.env.local` and set:
- `JWT_SECRET`
- `OPENAI_API_KEY` (optional if you need AI features)

## 2) Install dependencies

```bash
npm install
```

```bash
cd mini-services/dataforge-backend
pip install -r requirements.txt
cd ../..
```

## 3) Initialize Prisma (first time)

```bash
npx prisma generate
npx prisma db push
```

## 4) Start backend and frontend (2 terminals)

Terminal A (backend):
```bash
npm run dev:backend
```

Terminal B (frontend):
```bash
npm run dev:frontend
```

## 5) Verify
- Frontend: http://localhost:3000
- Backend health: http://localhost:3001/health

## Notes
- Frontend API routes use `BACKEND_URL` from env, so local and Docker both work.
- If backend starts but frontend cannot reach it, verify `BACKEND_URL=http://localhost:3001` in `.env.local`.