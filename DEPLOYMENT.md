# Production Deployment (Docker Compose)

This repository now includes:
- `Dockerfile.frontend`
- `Dockerfile.backend`
- `docker-compose.production.yml`
- `.env.production.example`

## 1) Prepare environment

```bash
cp .env.production.example .env.production
```

Edit `.env.production` and set at least:
- `JWT_SECRET`
- `OPENAI_API_KEY` (if LLM features are needed)

## 2) Build and run

```bash
docker compose -f docker-compose.production.yml build
docker compose -f docker-compose.production.yml up -d
```

## 3) Initialize Prisma schema (first deploy)

```bash
docker compose -f docker-compose.production.yml exec frontend npx prisma db push
```

If you maintain migrations in `prisma/migrations`, use:

```bash
docker compose -f docker-compose.production.yml exec frontend npx prisma migrate deploy
```

## 4) Verify services

```bash
curl http://localhost:3000
curl http://localhost:3001/health
```

## 5) Update deployment

```bash
git pull
docker compose -f docker-compose.production.yml build
docker compose -f docker-compose.production.yml up -d
```

## Notes
- Frontend routes now use `BACKEND_URL` env var instead of hardcoded `localhost:3001`.
- Persisted data directories are mounted from host:
  - `db/`, `data/`, `warehouse/`, `reports/`, `pipelines/`, `dbt_project/`, `tenants/`, `upload/`
- Put Caddy or Nginx in front of `:3000` for HTTPS.