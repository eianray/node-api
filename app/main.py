"""
Meridian GIS API — v0.2.0
Machine-native spatial data processing. For AI agents and developers alike.

Payment: x402 micropayments in USDC on Base.
No accounts. No API keys. No credit cards.
"""
import asyncio
import json
import time
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, File, Form, Header, HTTPException, Request, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response

from app.billing.x402 import (
    OPERATION_PRICES,
    OPERATION_DESCRIPTIONS,
    build_payment_required,
    require_payment,
)
from app.config import get_settings
from app.db import init_db, log_operation
from app.jobs import (
    extend_db_schema, create_job, get_job, complete_job, fail_job, mark_running,
    run_cleanup_loop
)
from app.operations.buffer    import run_buffer
from app.operations.clip      import run_clip
from app.operations.convert   import run_convert
from app.operations.dxf       import run_dxf_convert
from app.operations.reproject import run_reproject
from app.operations.schema    import run_schema
from app.operations.topology  import run_union, run_intersect, run_difference
from app.operations.validate  import run_validate, run_repair

settings = get_settings()

app = FastAPI(
    title="Node API",
    description=(
        "Machine-native spatial data processing. Convert, reproject, validate, "
        "repair, inspect, and clip vector spatial data.\n\n"
        "**Payment:** x402 micropayments in USDC on Base. "
        "No accounts, no API keys, no subscriptions.\n\n"
        "**How it works:**\n"
        "1. Send a request — receive a `402 Payment Required` response with USDC payment details\n"
        "2. Pay the specified amount on Base (EIP-3009 signed transfer)\n"
        "3. Re-send the request with your `X-PAYMENT` header\n"
        "4. Receive your processed spatial data\n\n"
        "Spec: [x402.org](https://x402.org)"
    ),
    version="0.3.1",
    servers=[{"url": "https://nodeapi.ai", "description": "Production"}],
    contact={"name": "DrawBridge / Planetary Modeling", "url": "https://drawbridgegis.com"},
    openapi_tags=[
        {"name": "Operations", "description": "Spatial data processing — all require x402 payment"},
        {"name": "Info",       "description": "Pricing, health, service info"},
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*", "X-PAYMENT", "X-PAYMENT-RESPONSE"],
    expose_headers=["X-PAYMENT-RESPONSE", "X-Meridian-Payer"],
)


@app.on_event("startup")
async def startup():
    init_db()
    extend_db_schema()
    asyncio.create_task(run_cleanup_loop())


# ── Info ─────────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
def root():
    from fastapi.responses import HTMLResponse
    pricing_rows = "\n".join(
        f"<tr><td>{op}</td><td>{OPERATION_DESCRIPTIONS.get(op, '')}</td><td>${amt/1_000_000:.4f}</td></tr>"
        for op, amt in OPERATION_PRICES.items()
    )
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Node API — Spatial Data Processing</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
           background: #fff; color: #1a1a1a; line-height: 1.6; }}
    .wrap {{ max-width: 760px; margin: 0 auto; padding: 64px 24px; }}
    h1 {{ font-size: 1.75rem; font-weight: 600; letter-spacing: -0.02em; }}
    .sub {{ color: #555; margin-top: 8px; font-size: 1.05rem; }}
    .divider {{ border: none; border-top: 1px solid #e5e5e5; margin: 40px 0; }}
    h2 {{ font-size: 0.8rem; font-weight: 600; text-transform: uppercase;
          letter-spacing: 0.08em; color: #888; margin-bottom: 16px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
    th {{ text-align: left; padding: 6px 12px 6px 0; color: #888;
          font-weight: 500; font-size: 0.8rem; text-transform: uppercase;
          letter-spacing: 0.05em; border-bottom: 1px solid #e5e5e5; }}
    td {{ padding: 8px 12px 8px 0; border-bottom: 1px solid #f0f0f0; vertical-align: top; }}
    td:last-child {{ font-variant-numeric: tabular-nums; color: #555; white-space: nowrap; }}
    td:first-child {{ font-family: monospace; font-size: 0.85rem; color: #1a1a1a; }}
    .links {{ display: flex; gap: 24px; margin-top: 40px; }}
    .links a {{ color: #0066cc; text-decoration: none; font-size: 0.95rem; }}
    .links a:hover {{ text-decoration: underline; }}
    .payment {{ background: #f8f8f8; border-radius: 6px; padding: 20px 24px;
                font-size: 0.9rem; color: #444; margin-top: 40px; }}
    .payment code {{ font-family: monospace; background: #eee; padding: 1px 5px;
                     border-radius: 3px; font-size: 0.85rem; }}
    .ver {{ color: #bbb; font-size: 0.8rem; margin-top: 48px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Node API</h1>
    <p class="sub">Spatial data processing API. Convert, reproject, validate, repair, clip, and analyze vector GIS data.</p>

    <hr class="divider">

    <h2>Operations</h2>
    <table>
      <thead><tr><th>Endpoint</th><th>Description</th><th>Price (USDC)</th></tr></thead>
      <tbody>{pricing_rows}</tbody>
    </table>

    <div class="payment">
      <strong>Payment:</strong> x402 micropayments in USDC on Base. No accounts, no API keys, no subscriptions.<br>
      Send a request → receive a <code>402</code> with payment details → pay on Base → resend with <code>X-PAYMENT</code> header → receive your data.
    </div>

    <div class="links">
      <a href="/docs">API Reference →</a>
      <a href="/pricing">Pricing (JSON) →</a>
      <a href="/health">Health →</a>
    </div>

    <p class="ver">nodeapi.ai &mdash; v{settings.app_version}</p>
  </div>
</body>
</html>"""
    return HTMLResponse(content=html)

@app.get("/health", tags=["Info"], summary="Service health")
def health():
    return {"status": "ok", "version": settings.app_version}


@app.get("/pricing", tags=["Info"], summary="Operation pricing in USDC")
def pricing():
    """
    Returns per-operation pricing in USDC.
    All prices are in atomic USDC units (6 decimal places).
    Divide by 1,000,000 for dollar value. e.g. 5000 = $0.005
    """
    return {
        "network": "base",
        "asset": "USDC",
        "asset_decimals": 6,
        "prices": {
            op: {
                "atomic_units": amount,
                "usd": f"${amount / 1_000_000:.4f}",
            }
            for op, amount in OPERATION_PRICES.items()
        },
    }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _file_response(out_bytes: bytes, out_filename: str, media_type: str,
                   payer: str = "") -> Response:
    headers = {"Content-Disposition": f'attachment; filename="{out_filename}"'}
    if payer:
        headers["X-Meridian-Payer"] = payer
    return Response(content=out_bytes, media_type=media_type, headers=headers)


# ── Operations ───────────────────────────────────────────────────────────────

@app.post(
    "/convert",
    tags=["Operations"],
    summary="Convert between spatial vector formats",
    description=(
        "Convert a spatial file between formats.\n\n"
        "**Supported inputs:** GeoJSON, Shapefile (.zip), KML, GeoPackage, GDB\n\n"
        "**Supported outputs:** `geojson`, `shapefile`, `kml`, `gpkg`\n\n"
        "**Payment:** Include `X-PAYMENT` header with valid USDC payment proof. "
        "Omit to receive payment requirements."
    ),
)
async def convert(
    request: Request,
    file: UploadFile           = File(..., description="Spatial file to convert"),
    output_format: str         = Form(..., description="Target format: geojson | shapefile | kml | gpkg"),
    input_format: Optional[str] = Form(None, description="Source format (auto-detected if omitted)"),
    x_payment: Optional[str]   = Header(None, alias="X-PAYMENT"),
):
    payer, txhash = await require_payment(request, "convert", x_payment)

    t0 = time.monotonic()
    file_bytes = await file.read()
    if len(file_bytes) > settings.max_upload_mb * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"File exceeds {settings.max_upload_mb}MB limit")

    try:
        out_bytes, out_filename, media_type = run_convert(
            file_bytes, file.filename, input_format, output_format
        )
        duration_ms = int((time.monotonic() - t0) * 1000)
        log_operation("convert", input_format, output_format,
                      len(file_bytes), len(out_bytes), duration_ms, True,
                      payer_address=payer, tx_hash=txhash)
        return _file_response(out_bytes, out_filename, media_type, payer)
    except ValueError as e:
        duration_ms = int((time.monotonic() - t0) * 1000)
        log_operation("convert", input_format, output_format,
                      len(file_bytes), 0, duration_ms, False, str(e),
                      payer_address=payer, tx_hash=txhash)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {e}")


@app.post(
    "/reproject",
    tags=["Operations"],
    summary="Reproject spatial data to a different CRS",
    description=(
        "Transform coordinates from one CRS to another using EPSG codes.\n\n"
        "Source CRS is auto-detected from the file. Override with `source_epsg` "
        "if the file lacks a .prj or embedded CRS.\n\n"
        "**Payment:** Include `X-PAYMENT` header with valid USDC payment proof."
    ),
)
async def reproject(
    request: Request,
    file: UploadFile              = File(...),
    target_epsg: int              = Form(..., description="Target CRS as EPSG code, e.g. 4326"),
    source_epsg: Optional[int]    = Form(None, description="Override source CRS if file lacks .prj"),
    output_format: Optional[str]  = Form(None, description="Output format (defaults to input format)"),
    x_payment: Optional[str]      = Header(None, alias="X-PAYMENT"),
):
    payer, txhash = await require_payment(request, "reproject", x_payment)

    t0 = time.monotonic()
    file_bytes = await file.read()
    if len(file_bytes) > settings.max_upload_mb * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"File exceeds {settings.max_upload_mb}MB limit")

    try:
        out_bytes, out_filename, media_type = run_reproject(
            file_bytes, file.filename, target_epsg, source_epsg, output_format
        )
        duration_ms = int((time.monotonic() - t0) * 1000)
        log_operation("reproject", str(source_epsg), str(target_epsg),
                      len(file_bytes), len(out_bytes), duration_ms, True,
                      payer_address=payer, tx_hash=txhash)
        return _file_response(out_bytes, out_filename, media_type, payer)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {e}")


@app.post(
    "/validate",
    tags=["Operations"],
    summary="Validate vector geometry — returns JSON report",
    description=(
        "Check for invalid geometries, self-intersections, and degenerate rings.\n\n"
        "**Returns:** JSON report with `total_features`, `valid_count`, `invalid_count`, "
        "and an `issues` array with per-feature descriptions.\n\n"
        "To receive a repaired file, use `POST /repair` instead.\n\n"
        "**Payment:** Include `X-PAYMENT` header with valid USDC payment proof."
    ),
)
async def validate(
    request: Request,
    file: UploadFile         = File(...),
    x_payment: Optional[str] = Header(None, alias="X-PAYMENT"),
):
    payer, txhash = await require_payment(request, "validate", x_payment)

    t0 = time.monotonic()
    file_bytes = await file.read()
    if len(file_bytes) > settings.max_upload_mb * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"File exceeds {settings.max_upload_mb}MB limit")

    try:
        result = run_validate(file_bytes, file.filename)
        duration_ms = int((time.monotonic() - t0) * 1000)
        log_operation("validate", None, None, len(file_bytes), 0,
                      duration_ms, True, payer_address=payer, tx_hash=txhash)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {e}")


@app.post(
    "/repair",
    tags=["Operations"],
    summary="Repair invalid vector geometry — returns fixed spatial file",
    description=(
        "Validate and repair geometry using `shapely.make_valid()`. "
        "Fixes self-intersections, winding order issues, and degenerate rings.\n\n"
        "**Returns:** Repaired spatial file. "
        "Validation stats (`X-Meridian-Fixed-Count`, `X-Meridian-Total-Features`) "
        "are included as response headers.\n\n"
        "For a validation report only (no file), use `POST /validate`.\n\n"
        "**Payment:** Include `X-PAYMENT` header with valid USDC payment proof."
    ),
)
async def repair(
    request: Request,
    file: UploadFile              = File(...),
    output_format: Optional[str]  = Form(None, description="Output format (defaults to input format)"),
    x_payment: Optional[str]      = Header(None, alias="X-PAYMENT"),
):
    payer, txhash = await require_payment(request, "repair", x_payment)

    t0 = time.monotonic()
    file_bytes = await file.read()
    if len(file_bytes) > settings.max_upload_mb * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"File exceeds {settings.max_upload_mb}MB limit")

    try:
        out_bytes, out_filename, media_type, stats = run_repair(
            file_bytes, file.filename, output_format
        )
        duration_ms = int((time.monotonic() - t0) * 1000)
        log_operation("repair", None, output_format, len(file_bytes), len(out_bytes),
                      duration_ms, True, payer_address=payer, tx_hash=txhash)
        headers = {
            "Content-Disposition":         f'attachment; filename="{out_filename}"',
            "X-Meridian-Total-Features":   str(stats["total_features"]),
            "X-Meridian-Fixed-Count":      str(stats["fixed_count"]),
            "X-Meridian-Payer":            payer,
        }
        return Response(content=out_bytes, media_type=media_type, headers=headers)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {e}")


@app.post(
    "/schema",
    tags=["Operations"],
    summary="Extract attribute schema and metadata — no geometry download",
    description=(
        "Inspect a spatial file: field names and types, CRS, geometry type, "
        "feature count, and bounding box. No geometry processing — fast and cheap.\n\n"
        "**Payment:** Include `X-PAYMENT` header with valid USDC payment proof."
    ),
)
async def schema(
    request: Request,
    file: UploadFile         = File(...),
    x_payment: Optional[str] = Header(None, alias="X-PAYMENT"),
):
    payer, txhash = await require_payment(request, "schema", x_payment)

    t0 = time.monotonic()
    file_bytes = await file.read()
    if len(file_bytes) > settings.max_upload_mb * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"File exceeds {settings.max_upload_mb}MB limit")

    try:
        result = run_schema(file_bytes, file.filename)
        duration_ms = int((time.monotonic() - t0) * 1000)
        log_operation("schema", None, None, len(file_bytes), 0,
                      duration_ms, True, payer_address=payer, tx_hash=txhash)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {e}")


@app.post(
    "/clip",
    tags=["Operations"],
    summary="Clip spatial data to a bounding box or polygon mask",
    description=(
        "Clip features to a spatial extent.\n\n"
        "- `bbox`: JSON array `[minx, miny, maxx, maxy]` in the file's native CRS\n"
        "- `mask`: GeoJSON geometry string (Polygon or MultiPolygon, WGS84)\n\n"
        "One of `bbox` or `mask` is required.\n\n"
        "**Payment:** Include `X-PAYMENT` header with valid USDC payment proof."
    ),
)
async def clip(
    request: Request,
    file: UploadFile              = File(...),
    bbox: Optional[str]           = Form(None, description='JSON array: [minx, miny, maxx, maxy]'),
    mask: Optional[str]           = Form(None, description="GeoJSON Polygon geometry string"),
    output_format: Optional[str]  = Form(None),
    x_payment: Optional[str]      = Header(None, alias="X-PAYMENT"),
):
    import json as _json
    payer, txhash = await require_payment(request, "clip", x_payment)

    t0 = time.monotonic()
    file_bytes = await file.read()
    if len(file_bytes) > settings.max_upload_mb * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"File exceeds {settings.max_upload_mb}MB limit")

    bbox_parsed = None
    if bbox:
        try:
            bbox_parsed = _json.loads(bbox)
        except Exception:
            raise HTTPException(status_code=400, detail="bbox must be a JSON array: [minx, miny, maxx, maxy]")

    try:
        out_bytes, out_filename, media_type = run_clip(
            file_bytes, file.filename, bbox_parsed, mask, output_format
        )
        duration_ms = int((time.monotonic() - t0) * 1000)
        log_operation("clip", None, output_format, len(file_bytes), len(out_bytes),
                      duration_ms, True, payer_address=payer, tx_hash=txhash)
        return _file_response(out_bytes, out_filename, media_type, payer)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {e}")

# ── DXF ──────────────────────────────────────────────────────────────────────

@app.post(
    "/dxf",
    tags=["Operations"],
    summary="Convert DXF/CAD file to vector spatial format",
    description=(
        "Extract geometry from DXF files (LINE, LWPOLYLINE, POLYLINE, CIRCLE, ARC, HATCH, SPLINE) "
        "and convert to GeoJSON, Shapefile, KML, or GeoPackage.\n\n"
        "**Important:** DXF files carry no CRS. Provide `source_epsg` if you know the "
        "coordinate system. Without it, raw DXF coordinates are preserved as-is.\n\n"
        "For large files this runs as an async job — response is `202 Accepted` "
        "with a `job_id`. Poll `GET /jobs/{job_id}` for the result.\n\n"
        "**Payment:** Include `X-PAYMENT` header with valid USDC payment proof."
    ),
)
async def dxf(
    request: Request,
    background_tasks: BackgroundTasks,
    file: UploadFile              = File(...),
    output_format: str            = Form(..., description="Target format: geojson | shapefile | kml | gpkg"),
    source_epsg: Optional[int]    = Form(None, description="Assign CRS to output (DXF has no CRS)"),
    layer_filter: Optional[str]   = Form(None, description="JSON array of DXF layer names to include"),
    entity_types: Optional[str]   = Form(None, description="JSON array of entity types: LINE, LWPOLYLINE, etc."),
    x_payment: Optional[str]      = Header(None, alias="X-PAYMENT"),
):
    payer, txhash = await require_payment(request, "dxf", x_payment)

    file_bytes = await file.read()
    if len(file_bytes) > settings.max_upload_mb * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"File exceeds {settings.max_upload_mb}MB limit")

    layers = json.loads(layer_filter) if layer_filter else None
    etypes = json.loads(entity_types) if entity_types else None

    # Large files → async job
    ASYNC_THRESHOLD = 5 * 1024 * 1024  # 5MB
    if len(file_bytes) > ASYNC_THRESHOLD:
        job_id = create_job("dxf", payer, txhash)

        async def _run():
            mark_running(job_id)
            t0 = time.monotonic()
            try:
                out_bytes, out_fn, mime, stats = run_dxf_convert(
                    file_bytes, file.filename, output_format, source_epsg, layers, etypes
                )
                complete_job(job_id, out_bytes, out_fn, mime, stats)
                log_operation("dxf", "dxf", output_format, len(file_bytes), len(out_bytes),
                              int((time.monotonic()-t0)*1000), True, payer_address=payer, tx_hash=txhash)
            except Exception as e:
                fail_job(job_id, str(e))
                log_operation("dxf", "dxf", output_format, len(file_bytes), 0, 0, False,
                              str(e), payer_address=payer, tx_hash=txhash)

        background_tasks.add_task(_run)
        return JSONResponse(
            status_code=202,
            content={"job_id": job_id, "status": "pending",
                     "poll_url": f"/jobs/{job_id}",
                     "message": "Large DXF file queued. Poll /jobs/{job_id} for result."}
        )

    # Small files → synchronous
    t0 = time.monotonic()
    try:
        out_bytes, out_fn, media_type, stats = run_dxf_convert(
            file_bytes, file.filename, output_format, source_epsg, layers, etypes
        )
        duration_ms = int((time.monotonic() - t0) * 1000)
        log_operation("dxf", "dxf", output_format, len(file_bytes), len(out_bytes),
                      duration_ms, True, payer_address=payer, tx_hash=txhash)
        headers = {
            "Content-Disposition":        f'attachment; filename="{out_fn}"',
            "X-Meridian-Feature-Count":   str(stats["feature_count"]),
            "X-Meridian-Layer-Count":     str(stats["layer_count"]),
            "X-Meridian-Payer":           payer,
        }
        if stats.get("warnings"):
            headers["X-Meridian-Warnings"] = "; ".join(stats["warnings"])
        return Response(content=out_bytes, media_type=media_type, headers=headers)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {e}")


# ── Jobs ─────────────────────────────────────────────────────────────────────

@app.get(
    "/jobs/{job_id}",
    tags=["Operations"],
    summary="Poll async job status or retrieve result",
    description=(
        "Poll the status of an async job (created by large DXF conversions).\n\n"
        "- `pending` / `running` → job not ready, poll again\n"
        "- `done` → result file is returned directly as a download\n"
        "- `failed` → error detail in response body"
    ),
)
async def poll_job(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job["status"] in ("pending", "running"):
        return JSONResponse(content={
            "job_id": job_id,
            "status": job["status"],
            "operation": job["operation"],
            "created_at": job["created_at"].isoformat() if job["created_at"] else None,
        })

    if job["status"] == "failed":
        return JSONResponse(
            status_code=500,
            content={"job_id": job_id, "status": "failed", "error": job["error"]}
        )

    # Done — return the file
    meta = job.get("result_meta") or {}
    headers = {
        "Content-Disposition": f'attachment; filename="{job["result_name"]}"',
        "X-Meridian-Job-Id":   job_id,
    }
    for k, v in meta.items():
        headers[f"X-Meridian-{k.replace('_','-').title()}"] = str(v)

    return Response(
        content=bytes(job["result_bytes"]),
        media_type=job["result_mime"],
        headers=headers,
    )


# ── Buffer ────────────────────────────────────────────────────────────────────

@app.post(
    "/buffer",
    tags=["Operations"],
    summary="Buffer features by distance in meters",
    description=(
        "Generate buffers around all features. Distance is in **meters**. "
        "Automatically reprojects to UTM for accuracy, then back to original CRS.\n\n"
        "Cap style: `round` | `flat` | `square`\n"
        "Join style: `round` | `mitre` | `bevel`\n\n"
        "**Payment:** Include `X-PAYMENT` header with valid USDC payment proof."
    ),
)
async def buffer(
    request: Request,
    file: UploadFile              = File(...),
    distance_meters: float        = Form(..., description="Buffer distance in meters"),
    output_format: Optional[str]  = Form(None),
    cap_style: str                = Form("round"),
    join_style: str               = Form("round"),
    resolution: int               = Form(16),
    source_epsg: Optional[int]    = Form(None, description="Assign CRS if file lacks one (e.g. DXF output)"),
    x_payment: Optional[str]      = Header(None, alias="X-PAYMENT"),
):
    payer, txhash = await require_payment(request, "buffer", x_payment)

    t0 = time.monotonic()
    file_bytes = await file.read()
    if len(file_bytes) > settings.max_upload_mb * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"File exceeds {settings.max_upload_mb}MB limit")

    try:
        out_bytes, out_fn, media_type = run_buffer(
            file_bytes, file.filename, distance_meters, output_format,
            cap_style, join_style, resolution, source_epsg
        )
        duration_ms = int((time.monotonic() - t0) * 1000)
        log_operation("buffer", None, output_format, len(file_bytes), len(out_bytes),
                      duration_ms, True, payer_address=payer, tx_hash=txhash)
        return _file_response(out_bytes, out_fn, media_type, payer)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {e}")


# ── Topology ──────────────────────────────────────────────────────────────────

def _topo_endpoint(operation: str, run_fn, description: str):
    """Factory to reduce boilerplate for the three topology endpoints."""
    pass  # defined inline below


@app.post(
    "/union",
    tags=["Operations"],
    summary="Union — combine all features from two layers",
    description=(
        "Merge all features from `layer_a` and `layer_b` into one output. "
        "CRS of layer_b is automatically aligned to layer_a.\n\n"
        "Set `dissolve=true` to merge all geometries into a single dissolved feature.\n\n"
        "**Payment:** Include `X-PAYMENT` header with valid USDC payment proof."
    ),
)
async def union(
    request: Request,
    layer_a: UploadFile           = File(..., description="First spatial layer"),
    layer_b: UploadFile           = File(..., description="Second spatial layer"),
    output_format: Optional[str]  = Form(None),
    dissolve: bool                = Form(False),
    x_payment: Optional[str]      = Header(None, alias="X-PAYMENT"),
):
    payer, txhash = await require_payment(request, "union", x_payment)
    t0 = time.monotonic()
    bytes_a = await layer_a.read()
    bytes_b = await layer_b.read()
    try:
        out_bytes, out_fn, media_type, topo_stats = run_union(
            bytes_a, layer_a.filename, bytes_b, layer_b.filename, output_format, dissolve
        )
        duration_ms = int((time.monotonic() - t0) * 1000)
        log_operation("union", None, output_format,
                      len(bytes_a)+len(bytes_b), len(out_bytes), duration_ms, True,
                      payer_address=payer, tx_hash=txhash)
        hdrs = {"Content-Disposition": f'attachment; filename="{out_fn}"',
                "X-Meridian-Payer": payer,
                "X-Meridian-Total-Features": str(topo_stats.get("total_features", "")),
                "X-Meridian-Layer-Count": str(topo_stats.get("layer_count", 1))}
        return Response(content=out_bytes, media_type=media_type, headers=hdrs)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {e}")


@app.post(
    "/intersect",
    tags=["Operations"],
    summary="Intersect — areas common to both layers",
    description=(
        "Return the spatial intersection of `layer_a` and `layer_b`. "
        "Attributes from layer_a are preserved. Returns 400 if no overlap exists.\n\n"
        "**Payment:** Include `X-PAYMENT` header with valid USDC payment proof."
    ),
)
async def intersect(
    request: Request,
    layer_a: UploadFile           = File(...),
    layer_b: UploadFile           = File(...),
    output_format: Optional[str]  = Form(None),
    x_payment: Optional[str]      = Header(None, alias="X-PAYMENT"),
):
    payer, txhash = await require_payment(request, "intersect", x_payment)
    t0 = time.monotonic()
    bytes_a = await layer_a.read()
    bytes_b = await layer_b.read()
    try:
        out_bytes, out_fn, media_type, topo_stats = run_intersect(
            bytes_a, layer_a.filename, bytes_b, layer_b.filename, output_format
        )
        duration_ms = int((time.monotonic() - t0) * 1000)
        log_operation("intersect", None, output_format,
                      len(bytes_a)+len(bytes_b), len(out_bytes), duration_ms, True,
                      payer_address=payer, tx_hash=txhash)
        hdrs = {"Content-Disposition": f'attachment; filename="{out_fn}"',
                "X-Meridian-Payer": payer,
                "X-Meridian-Total-Features": str(topo_stats.get("total_features", "")),
                "X-Meridian-Layer-Count": str(topo_stats.get("layer_count", 1))}
        return Response(content=out_bytes, media_type=media_type, headers=hdrs)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {e}")


@app.post(
    "/difference",
    tags=["Operations"],
    summary="Difference — parts of layer_a not covered by layer_b",
    description=(
        "Return features in `layer_a` that do NOT overlap `layer_b`. "
        "Equivalent to: A minus (A ∩ B). Returns 400 if A is entirely covered by B.\n\n"
        "**Payment:** Include `X-PAYMENT` header with valid USDC payment proof."
    ),
)
async def difference(
    request: Request,
    layer_a: UploadFile           = File(...),
    layer_b: UploadFile           = File(...),
    output_format: Optional[str]  = Form(None),
    x_payment: Optional[str]      = Header(None, alias="X-PAYMENT"),
):
    payer, txhash = await require_payment(request, "difference", x_payment)
    t0 = time.monotonic()
    bytes_a = await layer_a.read()
    bytes_b = await layer_b.read()
    try:
        out_bytes, out_fn, media_type, topo_stats = run_difference(
            bytes_a, layer_a.filename, bytes_b, layer_b.filename, output_format
        )
        duration_ms = int((time.monotonic() - t0) * 1000)
        log_operation("difference", None, output_format,
                      len(bytes_a)+len(bytes_b), len(out_bytes), duration_ms, True,
                      payer_address=payer, tx_hash=txhash)
        hdrs = {"Content-Disposition": f'attachment; filename="{out_fn}"',
                "X-Meridian-Payer": payer,
                "X-Meridian-Total-Features": str(topo_stats.get("total_features", "")),
                "X-Meridian-Layer-Count": str(topo_stats.get("layer_count", 1))}
        return Response(content=out_bytes, media_type=media_type, headers=hdrs)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {e}")
