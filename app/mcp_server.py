"""
Meridian MCP Server

Exposes Meridian's spatial operations as Model Context Protocol (MCP) tools.
AI agents (Claude, GPT, and MCP-compatible systems) can discover and call
Meridian operations without reading documentation.

Each tool accepts spatial data as base64-encoded file content.
Results are returned as base64-encoded output files with metadata.

Usage:
  python -m app.mcp_server

Registration:
  Add to Claude Desktop config:
    {
      "mcpServers": {
        "meridian": {
          "command": "/path/to/venv/bin/python",
          "args": ["-m", "app.mcp_server"],
          "cwd": "/path/to/meridian-api"
        }
      }
    }
"""

import asyncio
import base64
import json
import sys
from typing import Any, Optional

import httpx

try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp import types as mcp_types
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False

# Meridian API base URL (local by default; update for production)
MERIDIAN_BASE = "http://localhost:8100"
# In dev mode x402 is bypassed with any non-empty payment header
DEV_PAYMENT_HEADER = "mcp-devmode"

TOOL_DEFINITIONS = [
    {
        "name": "meridian_convert",
        "description": (
            "Convert a spatial vector file between formats. "
            "Supports: GeoJSON ↔ Shapefile ↔ KML ↔ GeoPackage ↔ GDB (read). "
            "Input file should be base64-encoded. "
            "Returns base64-encoded output file and metadata."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_b64":      {"type": "string", "description": "Base64-encoded spatial file"},
                "filename":      {"type": "string", "description": "Original filename (e.g. data.geojson)"},
                "output_format": {"type": "string", "enum": ["geojson", "shapefile", "kml", "gpkg"],
                                  "description": "Target format"},
                "input_format":  {"type": "string", "description": "Source format (auto-detected if omitted)"},
            },
            "required": ["file_b64", "filename", "output_format"],
        },
    },
    {
        "name": "meridian_reproject",
        "description": (
            "Reproject a spatial file to a different coordinate reference system. "
            "Specify target_epsg as an integer EPSG code (e.g. 4326 for WGS84, 3857 for Web Mercator). "
            "Source CRS is auto-detected from the file."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_b64":     {"type": "string", "description": "Base64-encoded spatial file"},
                "filename":     {"type": "string"},
                "target_epsg":  {"type": "integer", "description": "Target CRS EPSG code"},
                "source_epsg":  {"type": "integer", "description": "Override source CRS if file lacks .prj"},
                "output_format": {"type": "string", "enum": ["geojson", "shapefile", "kml", "gpkg"]},
            },
            "required": ["file_b64", "filename", "target_epsg"],
        },
    },
    {
        "name": "meridian_validate",
        "description": (
            "Validate vector geometry. Returns a JSON report with feature count, "
            "valid/invalid counts, and per-feature issue descriptions. "
            "Does not return a file — use meridian_repair for that."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_b64": {"type": "string"},
                "filename": {"type": "string"},
            },
            "required": ["file_b64", "filename"],
        },
    },
    {
        "name": "meridian_repair",
        "description": (
            "Repair invalid vector geometry using make_valid(). "
            "Fixes self-intersections, winding order issues, degenerate rings. "
            "Returns base64-encoded repaired file and fix stats."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_b64":     {"type": "string"},
                "filename":     {"type": "string"},
                "output_format": {"type": "string", "enum": ["geojson", "shapefile", "kml", "gpkg"]},
            },
            "required": ["file_b64", "filename"],
        },
    },
    {
        "name": "meridian_schema",
        "description": (
            "Extract attribute schema and metadata from a spatial file without downloading geometry. "
            "Returns: field names and types, CRS (EPSG + WKT), geometry type, "
            "feature count, and bounding box. Fast and cheap — use this first to inspect an unknown file."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_b64": {"type": "string"},
                "filename": {"type": "string"},
            },
            "required": ["file_b64", "filename"],
        },
    },
    {
        "name": "meridian_clip",
        "description": (
            "Clip a spatial layer to a bounding box or polygon mask. "
            "Provide bbox as [minx, miny, maxx, maxy] in the file's native CRS, "
            "or mask as a GeoJSON Polygon geometry string in WGS84."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_b64":     {"type": "string"},
                "filename":     {"type": "string"},
                "bbox":         {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4,
                                 "description": "[minx, miny, maxx, maxy]"},
                "mask":         {"type": "string", "description": "GeoJSON Polygon geometry string"},
                "output_format": {"type": "string", "enum": ["geojson", "shapefile", "kml", "gpkg"]},
            },
            "required": ["file_b64", "filename"],
        },
    },
    {
        "name": "meridian_dxf",
        "description": (
            "Convert a DXF/CAD file to a vector spatial format. "
            "Extracts geometry from DXF entities (LINE, LWPOLYLINE, POLYLINE, CIRCLE, ARC, HATCH, SPLINE). "
            "DXF files have no embedded CRS — provide source_epsg if you know the coordinate system. "
            "Optionally filter by layer names or entity types."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_b64":      {"type": "string"},
                "filename":      {"type": "string"},
                "output_format": {"type": "string", "enum": ["geojson", "shapefile", "kml", "gpkg"]},
                "source_epsg":   {"type": "integer", "description": "Assign CRS to output (DXF has no CRS)"},
                "layer_filter":  {"type": "array", "items": {"type": "string"},
                                  "description": "Only extract from these DXF layer names"},
                "entity_types":  {"type": "array", "items": {"type": "string"},
                                  "description": "Only extract these entity types: LINE, LWPOLYLINE, etc."},
            },
            "required": ["file_b64", "filename", "output_format"],
        },
    },
    {
        "name": "meridian_buffer",
        "description": (
            "Generate buffers around all features by a specified distance in meters. "
            "Automatically reprojects to an appropriate metric CRS (UTM) for accurate results, "
            "then reprojects back to the original CRS. "
            "Cap style: round|flat|square. Join style: round|mitre|bevel."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_b64":        {"type": "string"},
                "filename":        {"type": "string"},
                "distance_meters": {"type": "number", "description": "Buffer distance in meters"},
                "output_format":   {"type": "string", "enum": ["geojson", "shapefile", "kml", "gpkg"]},
                "cap_style":       {"type": "string", "enum": ["round", "flat", "square"], "default": "round"},
                "join_style":      {"type": "string", "enum": ["round", "mitre", "bevel"], "default": "round"},
                "resolution":      {"type": "integer", "default": 16,
                                    "description": "Segments per quarter circle (higher = smoother)"},
            },
            "required": ["file_b64", "filename", "distance_meters"],
        },
    },
    {
        "name": "meridian_union",
        "description": "Combine all features from two spatial layers into one.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_a_b64":    {"type": "string", "description": "Base64-encoded layer A"},
                "filename_a":    {"type": "string"},
                "file_b_b64":    {"type": "string", "description": "Base64-encoded layer B"},
                "filename_b":    {"type": "string"},
                "output_format": {"type": "string", "enum": ["geojson", "shapefile", "kml", "gpkg"]},
                "dissolve":      {"type": "boolean", "default": False,
                                  "description": "Merge all geometries into a single dissolved feature"},
            },
            "required": ["file_a_b64", "filename_a", "file_b_b64", "filename_b"],
        },
    },
    {
        "name": "meridian_intersect",
        "description": "Return the spatial intersection of two layers — areas common to both.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_a_b64":    {"type": "string"},
                "filename_a":    {"type": "string"},
                "file_b_b64":    {"type": "string"},
                "filename_b":    {"type": "string"},
                "output_format": {"type": "string", "enum": ["geojson", "shapefile", "kml", "gpkg"]},
            },
            "required": ["file_a_b64", "filename_a", "file_b_b64", "filename_b"],
        },
    },
    {
        "name": "meridian_difference",
        "description": (
            "Return parts of layer_a that do NOT overlap layer_b. "
            "Equivalent to: A minus (A ∩ B)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_a_b64":    {"type": "string"},
                "filename_a":    {"type": "string"},
                "file_b_b64":    {"type": "string"},
                "filename_b":    {"type": "string"},
                "output_format": {"type": "string", "enum": ["geojson", "shapefile", "kml", "gpkg"]},
            },
            "required": ["file_a_b64", "filename_a", "file_b_b64", "filename_b"],
        },
    },
    {
        "name": "meridian_pricing",
        "description": "Return current per-operation pricing in USDC. No payment required.",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
]


async def _call_meridian(
    method: str,
    endpoint: str,
    files: Optional[dict] = None,
    data: Optional[dict] = None,
    params: Optional[dict] = None,
) -> tuple[int, bytes, dict]:
    """Make a request to the Meridian API. Returns (status_code, body_bytes, headers)."""
    headers = {"X-PAYMENT": DEV_PAYMENT_HEADER}
    async with httpx.AsyncClient(timeout=120.0) as client:
        if method == "GET":
            resp = await client.get(f"{MERIDIAN_BASE}{endpoint}", headers=headers, params=params)
        else:
            resp = await client.post(
                f"{MERIDIAN_BASE}{endpoint}",
                headers=headers,
                files=files,
                data=data,
            )
    return resp.status_code, resp.content, dict(resp.headers)


async def handle_tool(name: str, arguments: dict) -> list:
    """Route tool calls to Meridian endpoints. Returns MCP content list."""

    def b64_to_bytes(b64: str) -> bytes:
        return base64.b64decode(b64 + "=" * (-len(b64) % 4))

    def result_text(data: dict) -> list:
        return [mcp_types.TextContent(type="text", text=json.dumps(data, indent=2))]

    def result_file(status: int, body: bytes, headers: dict, op: str) -> list:
        if status != 200:
            try:
                detail = json.loads(body).get("detail", body.decode())
            except Exception:
                detail = body.decode(errors="replace")
            return result_text({"error": f"HTTP {status}", "detail": detail})
        b64 = base64.b64encode(body).decode()
        content_disposition = headers.get("content-disposition", "")
        filename = "output"
        if "filename=" in content_disposition:
            filename = content_disposition.split("filename=")[-1].strip('"')
        meta = {
            k.replace("x-meridian-", ""): v
            for k, v in headers.items() if k.startswith("x-meridian-")
        }
        return result_text({
            "operation": op,
            "status": "success",
            "output_filename": filename,
            "output_b64": b64,
            "size_bytes": len(body),
            **meta,
        })

    try:
        if name == "meridian_pricing":
            status, body, _ = await _call_meridian("GET", "/pricing")
            return result_text(json.loads(body))

        elif name == "meridian_schema":
            file_bytes = b64_to_bytes(arguments["file_b64"])
            status, body, hdrs = await _call_meridian("POST", "/schema",
                files={"file": (arguments["filename"], file_bytes)})
            if status == 200:
                return result_text(json.loads(body))
            return result_text({"error": f"HTTP {status}", "detail": json.loads(body)})

        elif name == "meridian_validate":
            file_bytes = b64_to_bytes(arguments["file_b64"])
            status, body, hdrs = await _call_meridian("POST", "/validate",
                files={"file": (arguments["filename"], file_bytes)})
            if status == 200:
                return result_text(json.loads(body))
            return result_text({"error": f"HTTP {status}", "detail": json.loads(body)})

        elif name == "meridian_convert":
            file_bytes = b64_to_bytes(arguments["file_b64"])
            data = {"output_format": arguments["output_format"]}
            if arguments.get("input_format"):
                data["input_format"] = arguments["input_format"]
            status, body, hdrs = await _call_meridian("POST", "/convert",
                files={"file": (arguments["filename"], file_bytes)}, data=data)
            return result_file(status, body, hdrs, "convert")

        elif name == "meridian_reproject":
            file_bytes = b64_to_bytes(arguments["file_b64"])
            data = {"target_epsg": str(arguments["target_epsg"])}
            if arguments.get("source_epsg"):
                data["source_epsg"] = str(arguments["source_epsg"])
            if arguments.get("output_format"):
                data["output_format"] = arguments["output_format"]
            status, body, hdrs = await _call_meridian("POST", "/reproject",
                files={"file": (arguments["filename"], file_bytes)}, data=data)
            return result_file(status, body, hdrs, "reproject")

        elif name == "meridian_repair":
            file_bytes = b64_to_bytes(arguments["file_b64"])
            data = {}
            if arguments.get("output_format"):
                data["output_format"] = arguments["output_format"]
            status, body, hdrs = await _call_meridian("POST", "/repair",
                files={"file": (arguments["filename"], file_bytes)}, data=data)
            return result_file(status, body, hdrs, "repair")

        elif name == "meridian_clip":
            file_bytes = b64_to_bytes(arguments["file_b64"])
            data = {}
            if arguments.get("bbox"):
                data["bbox"] = json.dumps(arguments["bbox"])
            if arguments.get("mask"):
                data["mask"] = arguments["mask"]
            if arguments.get("output_format"):
                data["output_format"] = arguments["output_format"]
            status, body, hdrs = await _call_meridian("POST", "/clip",
                files={"file": (arguments["filename"], file_bytes)}, data=data)
            return result_file(status, body, hdrs, "clip")

        elif name == "meridian_dxf":
            file_bytes = b64_to_bytes(arguments["file_b64"])
            data = {"output_format": arguments["output_format"]}
            if arguments.get("source_epsg"):
                data["source_epsg"] = str(arguments["source_epsg"])
            if arguments.get("layer_filter"):
                data["layer_filter"] = json.dumps(arguments["layer_filter"])
            if arguments.get("entity_types"):
                data["entity_types"] = json.dumps(arguments["entity_types"])
            status, body, hdrs = await _call_meridian("POST", "/dxf",
                files={"file": (arguments["filename"], file_bytes)}, data=data)
            return result_file(status, body, hdrs, "dxf")

        elif name == "meridian_buffer":
            file_bytes = b64_to_bytes(arguments["file_b64"])
            data = {
                "distance_meters": str(arguments["distance_meters"]),
                "cap_style":  arguments.get("cap_style", "round"),
                "join_style": arguments.get("join_style", "round"),
                "resolution": str(arguments.get("resolution", 16)),
            }
            if arguments.get("output_format"):
                data["output_format"] = arguments["output_format"]
            status, body, hdrs = await _call_meridian("POST", "/buffer",
                files={"file": (arguments["filename"], file_bytes)}, data=data)
            return result_file(status, body, hdrs, "buffer")

        elif name in ("meridian_union", "meridian_intersect", "meridian_difference"):
            endpoint_map = {
                "meridian_union":      "/union",
                "meridian_intersect":  "/intersect",
                "meridian_difference": "/difference",
            }
            bytes_a = b64_to_bytes(arguments["file_a_b64"])
            bytes_b = b64_to_bytes(arguments["file_b_b64"])
            data = {}
            if arguments.get("output_format"):
                data["output_format"] = arguments["output_format"]
            if name == "meridian_union" and arguments.get("dissolve"):
                data["dissolve"] = "true"
            status, body, hdrs = await _call_meridian(
                "POST", endpoint_map[name],
                files={
                    "layer_a": (arguments["filename_a"], bytes_a),
                    "layer_b": (arguments["filename_b"], bytes_b),
                },
                data=data,
            )
            return result_file(status, body, hdrs, name.replace("meridian_", ""))

        else:
            return result_text({"error": f"Unknown tool: {name}"})

    except Exception as e:
        return result_text({"error": str(e)})


async def main():
    if not MCP_AVAILABLE:
        print("ERROR: mcp package not installed. Run: pip install mcp", file=sys.stderr)
        sys.exit(1)

    server = Server("meridian")

    @server.list_tools()
    async def list_tools() -> list[mcp_types.Tool]:
        return [
            mcp_types.Tool(
                name=t["name"],
                description=t["description"],
                inputSchema=t["inputSchema"],
            )
            for t in TOOL_DEFINITIONS
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[mcp_types.TextContent]:
        return await handle_tool(name, arguments)

    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
