# ny_parcels_etl_gdb.py
# ─────────────────────────────────────────────────────────────────────────────
# New York Parcels ETL (Prefect) — FileGDB input
# - Input: Esri File Geodatabase (.gdb), layer auto-detected (or specify LAYER_NAME)
# - Exclude:
#     A) SWIS_SBL_ID is NULL/empty
#     B) PARCEL_ADDR or PARCEL_ADDRESS contains "water" (case-insensitive)
# - Rename / select:
#     COUNTY_NAME → county
#     SWIS_SBL_ID → parcel_id
#     PRIMARY_OWNER → owner_name
# - Add: state = "new york"
# - Compute geodesic acres from geometry (WGS84)
# - Dedupe: (1) by parcel_id, (2) by (preferred keys + geometry), (3) exact dupes
# - OUTPUT COLUMNS: parcel_id, owner_name, county, state, acres, geometry
# - Output: GeoParquet (and optional GPKG/ZIP Shapefile)
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import os
import re
import gc
import time
import logging
import warnings
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import pandas as pd
import geopandas as gpd
from shapely.geometry import base as shapely_base
from shapely.validation import make_valid
from shapely import wkb
from pyproj import Geod
from prefect import flow, task, get_run_logger

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
INPUT_GDB   = r"C:\Users\matheswaran\OneDrive - Cleantech Industry Resources\Working_Files\ETL\parcel\New York\Input\NYS_2024_Tax_Parcels_Public_2508.gdb"    # 🟢 set this
LAYER_NAME  = None                              # e.g., "NY_Parcels_2024"; leave None to auto-detect
OUT_PARQUET = r"C:\Users\matheswaran\OneDrive - Cleantech Industry Resources\Working_Files\ETL\parcel\New York\Output\NewYork_Parcels_clean.parquet"

WRITE_GPKG    = False
OUT_GPKG      = r"C:\path\to\NewYork_Parcels_clean.gpkg"
WRITE_SHP_ZIP = False
OUT_SHP_ZIP   = r"C:\path\to\NewYork_Parcels_clean.zip"

# Performance / geometry options
os.environ.setdefault("OGR_ORGANIZE_POLYGONS", "ONLY_CCW")
os.environ.setdefault("GDAL_NUM_THREADS", "ALL_CPUS")

# NY schema (case-insensitive)
REQUIRED_COLS = [
    "county_name",     # → county
    "swis_sbl_id",     # → parcel_id
    "primary_owner",   # → owner_name
]
OPTIONAL_COLS = [
    "parcel_addr", "parcel_address",
    "sbl", "print_key",
    "citytown_name", "muni_name",
]

warnings.filterwarnings("ignore", category=UserWarning, module="fiona")
logging.getLogger("fiona").setLevel(logging.ERROR)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers

def _find_ci(names: List[str], target: str) -> Optional[str]:
    low = {n.lower(): n for n in names}
    return low.get(target.lower())

def _map_cols_ci(gdf: gpd.GeoDataFrame,
                 required: List[str],
                 optional: List[str]) -> Dict[str, Optional[str]]:
    mapping: Dict[str, Optional[str]] = {}
    for col in required:
        real = _find_ci(list(gdf.columns), col)
        if real is None:
            raise RuntimeError(
                f"Missing required column '{col}' (case-insensitive). "
                f"Available: {list(gdf.columns)}"
            )
        mapping[col] = real
    for col in optional:
        mapping[col] = _find_ci(list(gdf.columns), col)
    return mapping

def _ensure_wgs84(gdf: gpd.GeoDataFrame, logger) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        logger.warning("Input has no CRS; assuming EPSG:4326. Set CRS explicitly if this is wrong.")
        gdf = gdf.set_crs(4326, allow_override=True)
    epsg = gdf.crs.to_epsg() if gdf.crs else None
    if epsg != 4326:
        logger.info(f"Reprojecting {epsg} → 4326 for geodesic area calc.")
        gdf = gdf.to_crs(4326)
    return gdf

def _fix_geoms(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf.copy()
    gdf["geometry"] = gdf.geometry.apply(
        lambda geom: make_valid(geom) if isinstance(geom, shapely_base.BaseGeometry) and not geom.is_valid else geom
    )
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    return gdf

def _geodesic_acres(gdf: gpd.GeoDataFrame) -> pd.Series:
    geod = Geod(ellps="WGS84")
    def area_one(geom):
        if geom is None or geom.is_empty:
            return 0.0
        a, _ = geod.geometry_area_perimeter(geom)  # m²
        return abs(a) / 4046.8564224
    return gdf.geometry.apply(area_one)

def _is_nullish(val) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and pd.isna(val):
        return True
    s = str(val).strip()
    return s == "" or s.lower() in {"nan", "none", "null"}

def _addr_contains_water(s: Optional[str]) -> bool:
    if s is None:
        return False
    return "water" in str(s).lower()

# GDB layer utils
def _choose_gdb_layer(path_gdb: str, logger, prefer_keywords=("parcel",)) -> str:
    """
    Choose a polygon layer from a .gdb. Prefers layer names containing keywords.
    """
    try:
        import pyogrio
        layers = pyogrio.list_layers(path_gdb)
        # layers: list of tuples (name, geometry_type, feature_count)
        # geometry_type can be None for mixed; treat as acceptable if polygon-like name
        polys = []
        for name, gtype, _ in layers:
            gt = (gtype or "").lower()
            if "polygon" in gt:
                polys.append((name, gtype))
        # prefer keyword match among polygons
        for kw in prefer_keywords:
            for name, gtype in polys:
                if kw.lower() in name.lower():
                    logger.info(f"[gdb] Selected layer by keyword '{kw}': {name} ({gtype})")
                    return name
        if polys:
            logger.info(f"[gdb] Selected first polygon layer: {polys[0][0]} ({polys[0][1]})")
            return polys[0][0]
        # fallback: first layer
        if layers:
            logger.warning("[gdb] No polygon layer found; falling back to first layer.")
            return layers[0][0]
        raise RuntimeError("No layers found in GDB.")
    except Exception as e:
        logger.warning(f"[gdb] pyogrio.list_layers failed ({e!r}); trying fiona.")
        try:
            import fiona
            layer_names = fiona.listlayers(path_gdb)
            # We cannot check geometry type easily without opening; pick by keyword then first
            for kw in prefer_keywords:
                for name in layer_names:
                    if kw.lower() in name.lower():
                        logger.info(f"[gdb] Selected layer by keyword '{kw}': {name}")
                        return name
            if layer_names:
                logger.info(f"[gdb] Selected first layer: {layer_names[0]}")
                return layer_names[0]
            raise RuntimeError("No layers found in GDB (fiona).")
        except Exception as e2:
            raise RuntimeError(f"Unable to enumerate GDB layers: {e2!r}") from e2

# ─────────────────────────────────────────────────────────────────────────────
# Tasks

@task(retries=1, retry_delay_seconds=5)
def read_gdb(path_gdb: str, layer_name: Optional[str]) -> gpd.GeoDataFrame:
    logger = get_run_logger()
    t0 = time.time()
    if not layer_name:
        layer_name = _choose_gdb_layer(path_gdb, logger)
    logger.info(f"[read] Reading GDB: {path_gdb}, layer: {layer_name}")
    try:
        import pyogrio
        gdf = pyogrio.read_dataframe(path_gdb, layer=layer_name, max_workers=os.cpu_count() or 4)
        if not isinstance(gdf, gpd.GeoDataFrame):
            gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=getattr(gdf, "crs", None))
        logger.info("[read] Used pyogrio fast path.")
    except Exception as e:
        logger.warning(f"[read] pyogrio failed ({e!r}); falling back to GeoPandas/fiona.")
        gdf = gpd.read_file(path_gdb, layer=layer_name)
    logger.info(f"[read] Rows={len(gdf):,}, CRS={gdf.crs}, took {time.time()-t0:.2f}s")
    return gdf

@task
def sanitize_and_map_columns(gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, Dict[str, Optional[str]]]:
    logger = get_run_logger()
    t0 = time.time()
    gdf = _fix_geoms(gdf)
    gdf = _ensure_wgs84(gdf, logger)
    mapping = _map_cols_ci(gdf, REQUIRED_COLS, OPTIONAL_COLS)
    logger.info(f"[sanitize] Geometry fixed; columns mapped: {mapping} (took {time.time()-t0:.2f}s)")
    return gdf, mapping

@task
def filter_exclusions(gdf: gpd.GeoDataFrame, mapping: Dict[str, Optional[str]]) -> gpd.GeoDataFrame:
    """
    Exclude rows if:
      - SWIS_SBL_ID is null/empty
      - PARCEL_ADDR (or PARCEL_ADDRESS) contains 'water' (case-insensitive)
    """
    logger = get_run_logger()
    t0 = time.time()
    before = len(gdf)

    swis_col = mapping["swis_sbl_id"]
    addr_col = mapping.get("parcel_addr") or mapping.get("parcel_address")

    mask_null_pid = gdf[swis_col].apply(_is_nullish)
    if addr_col and addr_col in gdf.columns:
        mask_water = gdf[addr_col].apply(_addr_contains_water)
    else:
        mask_water = pd.Series(False, index=gdf.index)

    gdf = gdf.loc[~(mask_null_pid | mask_water)].copy()
    logger.info(f"[filter] Exclusions removed {before - len(gdf):,} → kept {len(gdf):,} (took {time.time()-t0:.2f}s)")
    return gdf

@task
def add_and_rename_fields(gdf: gpd.GeoDataFrame, mapping: Dict[str, Optional[str]]) -> gpd.GeoDataFrame:
    """
    - Rename:
        COUNTY_NAME → county
        SWIS_SBL_ID → parcel_id
        PRIMARY_OWNER → owner_name
    - Add:
        state = "new york"
    """
    logger = get_run_logger()
    t0 = time.time()

    rename_map = {
        mapping["county_name"]: "county",
        mapping["swis_sbl_id"]: "parcel_id",
        mapping["primary_owner"]: "owner_name",
    }
    gdf = gdf.rename(columns=rename_map)

    # Normalize types
    for c in ("parcel_id", "owner_name", "county"):
        if c in gdf.columns:
            gdf[c] = gdf[c].astype("string")

    gdf["state"] = "new york"

    logger.info(f"[add/rename] Added state, renamed fields (took {time.time()-t0:.2f}s)")
    return gdf

@task
def compute_acres(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    logger = get_run_logger()
    t0 = time.time()
    gdf["acres"] = _geodesic_acres(gdf)
    logger.info(f"[area] Computed geodesic acres for {len(gdf):,} rows (took {time.time()-t0:.2f}s)")
    return gdf

@task
def dedupe_three_stage(
    gdf: gpd.GeoDataFrame,
    mapping: Dict[str, Optional[str]],
) -> gpd.GeoDataFrame:
    """
    Stage 1: by parcel_id
    Stage 2: by (preferred keys + geometry)
             Preference:
               (citytown_name AND sbl) OR
               (county / county_name AND sbl) OR
               (county / county_name AND print_key) OR
               geometry only (fallback)
    Stage 3: exact duplicate rows (all attrs + geometry)
    """
    logger = get_run_logger()
    t_all = time.time()
    start_rows = len(gdf)

    # Stage 1
    if "parcel_id" in gdf.columns:
        before = len(gdf)
        gdf = gdf.drop_duplicates(subset=["parcel_id"], keep="first")
        logger.info(f"[dedupe-1] by parcel_id: {before:,} → {len(gdf):,} (removed {before-len(gdf):,})")

    # Stage 2 keys (robust to renamed/original)
    keys: List[str] = []
    city_col = mapping.get("citytown_name") or mapping.get("muni_name")
    sbl_col = mapping.get("sbl")
    county_orig = mapping.get("county_name")
    county_renamed = "county"

    def col_exists(c: Optional[str]) -> bool:
        return bool(c) and c in gdf.columns

    if col_exists(city_col) and col_exists(sbl_col):
        keys = [city_col, sbl_col]
    elif (col_exists(county_renamed) and col_exists(sbl_col)):
        keys = [county_renamed, sbl_col]
    elif (col_exists(county_orig) and col_exists(sbl_col)):
        keys = [county_orig, sbl_col]
    else:
        print_key_col = mapping.get("print_key")
        if col_exists(county_renamed) and col_exists(print_key_col):
            keys = [county_renamed, print_key_col]
        elif col_exists(county_orig) and col_exists(print_key_col):
            keys = [county_orig, print_key_col]
        else:
            keys = []  # fallback to geometry only

    gdf["_geom_wkbhex"] = gdf.geometry.apply(lambda x: wkb.dumps(x, hex=True) if x is not None else None)
    subset2 = keys + ["_geom_wkbhex"] if keys else ["_geom_wkbhex"]
    before2 = len(gdf)
    gdf = gdf.drop_duplicates(subset=subset2, keep="first")
    gdf = gdf.drop(columns=["_geom_wkbhex"])
    logger.info(f"[dedupe-2] by {', '.join(subset2)}: {before2:,} → {len(gdf):,} (removed {before2-len(gdf):,})")

    # Stage 3 exact
    attrs = [c for c in gdf.columns if c != "geometry"]
    gdf["_geom_wkbhex"] = gdf.geometry.apply(lambda x: wkb.dumps(x, hex=True) if x is not None else None)
    before3 = len(gdf)
    gdf = gdf.drop_duplicates(subset=attrs + ["_geom_wkbhex"], keep="first")
    gdf = gdf.drop(columns=["_geom_wkbhex"])
    logger.info(f"[dedupe-3] exact (all attrs + geometry): {before3:,} → {len(gdf):,} (removed {before3-len(gdf):,})")

    logger.info(f"[dedupe] Total: {start_rows:,} → {len(gdf):,} "
                f"(removed {start_rows-len(gdf):,}) in {time.time()-t_all:.2f}s")
    return gdf

@task
def keep_minimal_schema_and_write(
    gdf: gpd.GeoDataFrame,
    out_parquet: str,
    write_gpkg: bool,
    out_gpkg: str,
    write_shp_zip: bool,
    out_shp_zip: str,
) -> None:
    """
    Enforce minimal output schema:
      parcel_id, owner_name, county, state, acres, geometry
    """
    logger = get_run_logger()
    t0 = time.time()

    keep_cols = ["parcel_id", "owner_name", "county", "state", "acres", "geometry"]
    missing = [c for c in keep_cols if c not in gdf.columns]
    if missing:
        raise RuntimeError(f"Missing required output columns: {missing}")

    gdf = gdf[keep_cols]

    Path(out_parquet).parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(out_parquet, index=False)
    logger.info(f"[write] GeoParquet → {out_parquet} ({len(gdf):,} rows) "
                f"[schema: parcel_id,owner_name,county,state,acres]")

    if write_gpkg:
        Path(out_gpkg).parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(out_gpkg, layer="ny_parcels_clean", driver="GPKG")
        logger.info(f"[write] GPKG → {out_gpkg}")

    if write_shp_zip:
        tmp_dir = Path(out_shp_zip).with_suffix("")
        if tmp_dir.exists():
            for p in tmp_dir.glob("*"):
                try: p.unlink()
                except Exception:
                    pass
        else:
            tmp_dir.mkdir(parents=True, exist_ok=True)

        shp_path = tmp_dir / "ny_parcels_clean.shp"
        gdf.to_file(shp_path, driver="ESRI Shapefile")

        import zipfile
        with zipfile.ZipFile(out_shp_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in tmp_dir.iterdir():
                zf.write(p, arcname=p.name)
        logger.info(f"[write] Shapefile ZIP → {out_shp_zip}")

    logger.info(f"[write] Completed writing in {time.time()-t0:.2f}s")

# ─────────────────────────────────────────────────────────────────────────────
# Flow

@flow(name="NY Parcels Clean ETL (GDB)")
def ny_parcels_clean_flow(
    input_gdb: str = INPUT_GDB,
    layer_name: Optional[str] = LAYER_NAME,
    out_parquet: str = OUT_PARQUET,
    write_gpkg: bool = WRITE_GPKG,
    out_gpkg: str = OUT_GPKG,
    write_shp_zip: bool = WRITE_SHP_ZIP,
    out_shp_zip: str = OUT_SHP_ZIP,
):
    logger = get_run_logger()
    t0 = time.time()

    # Read & map
    gdf = read_gdb(input_gdb, layer_name)
    gdf, mapping = sanitize_and_map_columns(gdf)

    # Exclusions
    gdf = filter_exclusions(gdf, mapping)

    # Rename + add fields
    gdf = add_and_rename_fields(gdf, mapping)

    # Acres
    gdf = compute_acres(gdf)

    # Dedupe (3 stages)
    gdf = dedupe_three_stage(gdf, mapping)

    # Minimal schema + write
    keep_minimal_schema_and_write(
        gdf,
        out_parquet=out_parquet,
        write_gpkg=write_gpkg,
        out_gpkg=out_gpkg,
        write_shp_zip=write_shp_zip,
        out_shp_zip=out_shp_zip,
    )

    del gdf
    gc.collect()
    logger.info(f"[flow] Finished in {time.time()-t0:.2f}s")

# ─────────────────────────────────────────────────────────────────────────────
# CLI entry
if __name__ == "__main__":
    ny_parcels_clean_flow()
