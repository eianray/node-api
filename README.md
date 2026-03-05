# Node API

**Version:** 0.4.0  
**Status:** Phase 2 Complete — Public  
**Live at:** [nodeapi.ai](https://nodeapi.ai) | [API Docs](https://nodeapi.ai/docs) | [MCP Endpoint](https://nodeapi.ai/mcp/sse)  
**Internal name:** Project Meridian

Machine-native spatial data processing API for AI agents. Convert, reproject, validate, clip, and transform vector GIS files — designed for autonomous agents, paid per-operation in USDC via x402.

No accounts. No API keys. No subscriptions.

---

## Endpoints

| Method | Path | Description | Price (USDC) |
|--------|------|-------------|--------------|
| GET | `/health` | Service health | free |
| GET | `/pricing` | Current operation prices | free |
| POST | `/convert` | Convert between spatial formats | $0.005 |
| POST | `/reproject` | Reproject to target EPSG | $0.003 |
| POST | `/validate` | Validate geometry, return report | $0.002 |
| POST | `/repair` | Repair geometry, return fixed file | $0.003 |
| POST | `/schema` | Extract attribute schema (no geometry) | $0.001 |
| POST | `/clip` | Clip to bounding box or polygon | $0.004 |
| POST | `/dxf` | Extract geometry from DXF/CAD files | $0.010 |
| POST | `/buffer` | Generate projected buffers | $0.004 |
| POST | `/union` | Union of two feature layers | $0.006 |
| POST | `/intersect` | Spatial intersection of two layers | $0.006 |
| POST | `/difference` | Spatial difference between layers | $0.006 |
| GET | `/jobs/{id}` | Poll async job status | free |

**Supported formats:** GeoJSON, Shapefile (.zip), GeoPackage, KML, GDB (read), DXF

---

## Payment (x402)

Every paid endpoint follows the x402 protocol:

1. Agent sends request (no payment header)
2. API responds `402 Payment Required` with USDC amount + wallet address on Base
3. Agent pays on-chain (~2s finality on Base L2)
4. Agent re-sends with `X-PAYMENT` header (signed transfer proof)
5. API verifies via Coinbase facilitator → processes → returns result

**Receiving wallet:** `0x87f12546bF32a999F80f47e70F7860a4cF1E5B74`  
**Network:** Base (Ethereum L2)  
**Asset:** USDC

---

## MCP Integration

Node API is registered in the [Anthropic MCP directory](https://github.com/modelcontextprotocol/servers).

### Remote (any agent, no install)
Connect to: `https://nodeapi.ai/mcp/sse`

### Local (Claude Desktop)
Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "node-api": {
      "command": "/path/to/venv/bin/python",
      "args": ["-m", "app.mcp_server"]
    }
  }
}
```

**MCP Tools (12):** `meridian_convert`, `meridian_reproject`, `meridian_validate`, `meridian_repair`, `meridian_schema`, `meridian_clip`, `meridian_dxf`, `meridian_buffer`, `meridian_union`, `meridian_intersect`, `meridian_difference`, `meridian_pricing`

---

## Architecture

- **FastAPI** (Python 3.12) — async, auto-generates OpenAPI spec at `/docs`
- **pyogrio + GDAL** — 200+ vector/raster formats
- **Shapely 2.x** — geometry validation, repair, topology operations
- **pyproj** — CRS reprojection (any EPSG pair)
- **PostgreSQL** — operations log (payer wallet + tx hash per request)
- **x402** — USDC micropayments on Base via Coinbase facilitator
- **MCP SDK 1.26.0** — stdio (local) + SSE (remote) transport
- **Cloudflare Tunnel** — `nodeapi.ai` → `localhost:8100`

## Infrastructure

- **Host:** Mac mini (Tier 0 — 0–1k req/day)
- **LaunchAgent:** `com.malko.meridian-api` (port 8100)
- **MCP SSE LaunchAgent:** `com.malko.meridian-mcp-sse` (port 8101, unused — SSE mounted on 8100 at `/mcp`)
- **Tunnel:** `~/.cloudflared/config.yml`

## Setup (dev)

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
cp .env.example .env
# Set WALLET_ADDRESS=0x0000000000000000000000000000000000000000 for dev mode (bypasses x402)
venv/bin/uvicorn app.main:app --reload --port 8100
```

## Roadmap

- **Phase 2 ✅ (Mar 4 2026):** DXF/CAD, buffer/topology ops, remote MCP SSE, x402 live, MCP directory PR submitted
- **Phase 3:** Vector tile generation, webhook/polling, Hetzner migration, CDN caching, batch API, Solana Pay for sub-cent ops

---

*A Planetary Modeling, Inc. / DrawBridge LLC product.*  
*Internal name: Project Meridian*
