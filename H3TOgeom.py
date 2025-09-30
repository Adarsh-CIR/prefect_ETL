#!/usr/bin/env python3
"""
Convert H3 res8 IDs to Polygon geometry and write a GeoParquet (no compression).

- Input:  Parquet file (or dataset directory) with a column like 'h3_res8_id'
- Output: GeoParquet file with:
    - 'geometry' column (WKB Polygon, EPSG:4326)
    - proper GeoParquet metadata
    - no compression

Usage:
  python h3_to_geoparquet.py \
      --input /path/to/input.parquet \
      --output /path/to/output.geoparquet \
      --h3-col h3_res8_id \
      --geom-col geometry \
      --batch-size 100000
"""

import argparse
import sys
from typing import Optional, List

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from shapely.geometry import Polygon
from shapely.wkb import dumps as wkb_dumps
import h3  # python-h3


def h3_to_wkb(h3_index: Optional[str]) -> Optional[bytes]:
    """Convert a single H3 index to WKB Polygon (lon/lat, EPSG:4326)."""
    if not h3_index:
        return None
    try:
        # geo_json=True returns [(lat, lon), ...] in CCW order on a sphere
        boundary_latlon = h3.h3_to_geo_boundary(h3_index, geo_json=True)
        ring = [(lon, lat) for (lat, lon) in boundary_latlon]  # shapely expects (x=lon, y=lat)
        poly = Polygon(ring)
        if not poly.is_valid:
            poly = poly.buffer(0)
        if poly.is_empty:
            return None
        return wkb_dumps(poly, hex=False)  # bytes
    except Exception:
        return None


def build_geo_metadata(geom_col: str) -> dict:
    """
    GeoParquet schema-level metadata block.
    Ref: https://github.com/opengeospatial/geoparquet
    """
    geo_meta = {
        "geo": {
            "version": "0.4.0",
            "primary_column": geom_col,
            "columns": {
                geom_col: {
                    "encoding": "WKB",
                    "geometry_types": ["Polygon"],   # H3 boundary → Polygon
                    "crs": "EPSG:4326",
                    "edges": "spherical",
                    "orientation": "counterclockwise",
                    # "bbox": [minx, miny, maxx, maxy]  # optional; omitted for streaming
                }
            }
        }
    }
    # Arrow wants keys/values to be bytes
    return {k.encode("utf-8"): pa.scalar(v).as_buffer().to_pybytes() for k, v in {
        "geo": pa.scalar(geo_meta["geo"]).to_string()
    }.items()}


def process(
    input_path: str,
    output_path: str,
    h3_col: str,
    geom_col: str,
    batch_size: int,
):
    dataset = ds.dataset(input_path, format="parquet")
    schema = dataset.schema

    if h3_col not in schema.names:
        raise ValueError(f"Input column '{h3_col}' not found. Available: {schema.names}")

    # Prepare output schema = input + binary geometry column
    out_fields = list(schema)
    out_fields.append(pa.field(geom_col, pa.binary()))
    out_schema = pa.schema(out_fields)

    # Attach GeoParquet metadata at the schema level
    geo_md = build_geo_metadata(geom_col)
    out_schema = out_schema.with_metadata(geo_md)

    # Writer with NO COMPRESSION
    writer = pq.ParquetWriter(
        where=output_path,
        schema=out_schema,
        compression=None,           # ← no compression
        use_dictionary=False,       # purely optional; avoids dict encoding
        write_statistics=True
    )

    total_rows = 0
    try:
        scanner = dataset.scan(batch_size=batch_size)
        for i, batch in enumerate(scanner.to_batches()):
            # Convert H3 → WKB geometry per batch
            h3_vals: List[Optional[str]] = batch.column(batch.schema.get_field_index(h3_col)).to_pylist()
            wkb_vals = [h3_to_wkb(h) for h in h3_vals]
            wkb_array = pa.array(wkb_vals, type=pa.binary())

            # Append geometry column
            arrays = [batch.column(j) for j in range(batch.num_columns)] + [wkb_array]
            names = list(batch.schema.names) + [geom_col]
            out_batch = pa.RecordBatch.from_arrays(arrays, names=names)

            # Write out immediately (streaming)
            writer.write_table(pa.Table.from_batches([out_batch]))
            total_rows += batch.num_rows

            if (i + 1) % 10 == 0:
                print(f"[info] processed ~{total_rows:,} rows...", file=sys.stderr)
    finally:
        writer.close()

    print(f"[done] wrote GeoParquet (no compression): {output_path}")
    print(f"[rows] total rows: {total_rows:,}")
    print(f"[geom] column: {geom_col} (WKB, EPSG:4326)")


def main():
    ap = argparse.ArgumentParser(description="Stream H3→geometry and write GeoParquet (no compression).")
    ap.add_argument("--input", required=True, help="Input Parquet file or directory")
    ap.add_argument("--output", required=True, help="Output GeoParquet file (e.g., .geoparquet)")
    ap.add_argument("--h3-col", default="h3_res8_id", help="Name of the H3 index column")
    ap.add_argument("--geom-col", default="geometry", help="Name of the output geometry column")
    ap.add_argument("--batch-size", type=int, default=100_000, help="Rows per batch (tune for RAM/CPU)")
    args = ap.parse_args()

    process(
        input_path=args.input,
        output_path=args.output,
        h3_col=args.h3_col,
        geom_col=args.geom_col,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
