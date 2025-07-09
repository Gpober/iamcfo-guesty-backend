# IAM CFO - Guesty Integration Backend

FastAPI backend that syncs Guesty reservation data to Supabase.

## Environment Variables Required:
- SUPABASE_URL
- SUPABASE_ANON_KEY

## Endpoints:
- POST /api/guesty/sync - Manual sync
- GET /api/dashboard/properties - Get properties
- GET /api/dashboard/reservations - Get reservations
- GET /api/dashboard/kpis - Get KPIs
