"""
Meridian GIS API — v0.1.0
Machine-native spatial data processing. For AI agents and developers alike.

Payment: x402 micropayments in USDC on Base.
No accounts. No API keys. No credit cards.
"""
import time
from typing import Optional

from fastapi import FastAPI, File, Form, Header, HTTPException, Request, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response

from app.billing.x402 import (
    OPERATION_PRICES,
    build_payment_required,
    require_payment,
)
from app.config import get_settings
from app.db import init_db, log_operation
from app.operations.clip      import run_clip
from app.operations.convert   import run_convert
from app.operations.reproject import run_reproject
from app.operations.schema    import run_schema
from app.operations.validate  import run_validate, run_repair

settings = get_settings()

app = FastAPI(
    title="Meridian GIS API",
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
    version="0.1.0",
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


# ── Info ─────────────────────────────────────────────────────────────────────

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
