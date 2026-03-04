"""
POST /union        — Merge two feature layers into one
POST /intersect    — Return features/areas common to both layers
POST /difference   — Return features in layer A that don't overlap layer B

All topology operations accept two file uploads (layer_a, layer_b).
CRS alignment is handled automatically — layer_b is reprojected to layer_a's CRS.
Results are returned in layer_a's CRS.
"""
import os
import shutil
import tempfile
from typing import Optional

import geopandas as gpd
from shapely.ops import unary_union

from app.operations.convert import _unpack_upload, _pack_shapefile, detect_format, DRIVER_MAP, WRITE_FORMATS


def _dominant_geom_type(gdf: gpd.GeoDataFrame) -> str:
    """Return the most common geometry type in a GeoDataFrame."""
    types = gdf.geometry.geom_type.value_counts()
    return types.index[0] if len(types) > 0 else "Polygon"


def _normalize_geom_type(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    If a GeoDataFrame has mixed geometry types, keep only the dominant type.
    geopandas.overlay() requires homogeneous input geometry types.
    """
    types = gdf.geometry.geom_type.unique()
    if len(types) <= 1:
        return gdf
    dominant = _dominant_geom_type(gdf)
    # Keep only features of the dominant type
    return gdf[gdf.geometry.geom_type == dominant].copy()


def _load_two(
    bytes_a: bytes, name_a: str,
    bytes_b: bytes, name_b: str,
    tmpdir: str,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Load two layers, align CRS to layer_a, normalize geometry types."""
    path_a = _unpack_upload(bytes_a, name_a, os.path.join(tmpdir, "a"))
    path_b = _unpack_upload(bytes_b, name_b, os.path.join(tmpdir, "b"))

    gdf_a = gpd.read_file(path_a)
    gdf_b = gpd.read_file(path_b)

    # Normalize mixed geometry types
    gdf_a = _normalize_geom_type(gdf_a)
    gdf_b = _normalize_geom_type(gdf_b)

    # Align CRS: reproject B to A
    if gdf_a.crs and gdf_b.crs and gdf_a.crs != gdf_b.crs:
        gdf_b = gdf_b.to_crs(gdf_a.crs)
    elif gdf_a.crs and not gdf_b.crs:
        gdf_b = gdf_b.set_crs(gdf_a.crs)

    return gdf_a, gdf_b


def _write_output(gdf: gpd.GeoDataFrame, fmt: str, stem: str, tmpdir: str) -> tuple[bytes, str, str]:
    driver, ext = DRIVER_MAP[fmt]

    if fmt == "shapefile":
        out_bytes = _pack_shapefile(gdf, tmpdir)
        return out_bytes, f"{stem}.zip", "application/zip"

    out_path = os.path.join(tmpdir, f"{stem}{ext}")
    gdf.to_file(out_path, driver=driver)
    with open(out_path, "rb") as f:
        out_bytes = f.read()

    media_type = {
        "geojson":    "application/geo+json",
        "kml":        "application/vnd.google-earth.kml+xml",
        "gpkg":       "application/geopackage+sqlite3",
        "geopackage": "application/geopackage+sqlite3",
    }.get(fmt, "application/octet-stream")

    return out_bytes, f"{stem}{ext}", media_type


def run_union(
    bytes_a: bytes, name_a: str,
    bytes_b: bytes, name_b: str,
    output_format: Optional[str],
    dissolve: bool,
) -> tuple[bytes, str, str]:
    """
    Union: combine all features from both layers into one.
    If dissolve=True, merge all geometries into a single dissolved feature.
    """
    fmt = (output_format or detect_format(name_a) or "geojson").lower()
    if fmt not in WRITE_FORMATS:
        fmt = "geojson"

    tmpdir = tempfile.mkdtemp(prefix="meridian_union_")
    os.makedirs(os.path.join(tmpdir, "a"), exist_ok=True)
    os.makedirs(os.path.join(tmpdir, "b"), exist_ok=True)

    try:
        gdf_a, gdf_b = _load_two(bytes_a, name_a, bytes_b, name_b, tmpdir)

        result = gpd.pd.concat([gdf_a, gdf_b], ignore_index=True)
        result = gpd.GeoDataFrame(result, geometry="geometry", crs=gdf_a.crs)

        if dissolve:
            dissolved = unary_union(result.geometry)
            result = gpd.GeoDataFrame(geometry=[dissolved], crs=gdf_a.crs)

        return _write_output(result, fmt, "union", tmpdir)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def run_intersect(
    bytes_a: bytes, name_a: str,
    bytes_b: bytes, name_b: str,
    output_format: Optional[str],
) -> tuple[bytes, str, str]:
    """
    Intersect: return the spatial intersection of the two layers.
    Attributes from layer_a are preserved.
    """
    fmt = (output_format or detect_format(name_a) or "geojson").lower()
    if fmt not in WRITE_FORMATS:
        fmt = "geojson"

    tmpdir = tempfile.mkdtemp(prefix="meridian_intersect_")
    os.makedirs(os.path.join(tmpdir, "a"), exist_ok=True)
    os.makedirs(os.path.join(tmpdir, "b"), exist_ok=True)

    try:
        gdf_a, gdf_b = _load_two(bytes_a, name_a, bytes_b, name_b, tmpdir)

        result = gpd.overlay(gdf_a, gdf_b, how="intersection", keep_geom_type=False)

        if result.empty:
            raise ValueError("Intersection is empty — the two layers do not overlap.")

        return _write_output(result, fmt, "intersection", tmpdir)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def run_difference(
    bytes_a: bytes, name_a: str,
    bytes_b: bytes, name_b: str,
    output_format: Optional[str],
) -> tuple[bytes, str, str]:
    """
    Difference: return parts of layer_a that do NOT overlap layer_b.
    Equivalent to: A minus (A ∩ B).
    """
    fmt = (output_format or detect_format(name_a) or "geojson").lower()
    if fmt not in WRITE_FORMATS:
        fmt = "geojson"

    tmpdir = tempfile.mkdtemp(prefix="meridian_diff_")
    os.makedirs(os.path.join(tmpdir, "a"), exist_ok=True)
    os.makedirs(os.path.join(tmpdir, "b"), exist_ok=True)

    try:
        gdf_a, gdf_b = _load_two(bytes_a, name_a, bytes_b, name_b, tmpdir)

        result = gpd.overlay(gdf_a, gdf_b, how="difference", keep_geom_type=False)

        if result.empty:
            raise ValueError("Difference is empty — layer_a is entirely covered by layer_b.")

        return _write_output(result, fmt, "difference", tmpdir)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
