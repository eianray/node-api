# Meridian GIS API

**Version:** 0.1.0  
**Status:** Phase 1 MVP  
**Host:** Mac mini → `meridian.drawbridgegis.com`

Machine-native spatial data processing. Convert, reproject, validate, inspect, and clip vector spatial data — designed for AI agents and developers.

---

## Phase 1 Endpoints

| Method | Path | Description | Credits |
|--------|------|-------------|---------|
| GET | `/health` | Service health | free |
| POST | `/auth/register` | Register email → get free API key + 10 credits | free |
| GET | `/account/credits` | Check credit balance | free |
| GET | `/billing/bundles` | List credit bundle options | free |
| POST | `/billing/checkout` | Create Stripe checkout session | free |
| POST | `/convert` | Convert between spatial formats | 1 |
| POST | `/reproject` | Reproject to target EPSG | 1 |
| POST | `/validate` | Validate + optionally repair geometry | 1 |
| POST | `/schema` | Extract schema/metadata (no geometry) | 1 |
| POST | `/clip` | Clip to bbox or polygon mask | 1 |

**OpenAPI docs:** `http://localhost:8100/docs`  
**ReDoc:** `http://localhost:8100/redoc`

---

## Setup

### 1. PostgreSQL
```bash
bash scripts/setup_db.sh
```

### 2. Environment
```bash
cp .env.example .env
# Fill in DATABASE_URL, STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET
```

### 3. Install dependencies
```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
```

### 4. Run (dev)
```bash
venv/bin/uvicorn app.main:app --reload --port 8100
```

### 5. Run (production via LaunchAgent)
```bash
cp com.malko.meridian-api.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.malko.meridian-api.plist
```

---

## Quick API Test

```bash
# Register and get free key
curl -X POST http://localhost:8100/auth/register \
  -F "email=test@example.com"

# Convert a GeoJSON to GeoPackage
curl -X POST http://localhost:8100/convert \
  -H "X-API-Key: mrd_YOUR_KEY_HERE" \
  -F "file=@yourfile.geojson" \
  -F "output_format=gpkg" \
  -o output.gpkg

# Check schema
curl -X POST http://localhost:8100/schema \
  -H "X-API-Key: mrd_YOUR_KEY_HERE" \
  -F "file=@yourfile.geojson"
```

---

## Architecture

- **FastAPI** — async, auto-generates OpenAPI spec
- **geopandas + pyogrio** — fast vector I/O (200+ formats via GDAL)
- **Shapely 2.x** — geometry validation/repair
- **pyproj** — CRS reprojection
- **PostgreSQL** — API keys + credit ledger + ops log
- **Stripe** — credit purchase

## Roadmap

- **Phase 2:** DXF/CAD conversion, buffer/topology ops, vector tiles, x402 payments, MCP registration
- **Phase 3:** Horizontal scaling, CDN caching, batch jobs, enterprise

---

*A Planetary Modeling / DrawBridge LLC product.*
