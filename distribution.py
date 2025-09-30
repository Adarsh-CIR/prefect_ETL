import os
from typing import Optional, List

import geopandas as gpd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from dotenv import load_dotenv
from shapely import wkb

# ───────── LOAD ENV FILE ─────────
load_dotenv()
DB_URL = os.getenv("DATABASE_URL_LOCAL")  # Use local for testing
if not DB_URL:
    raise ValueError("❌ DATABASE_URL not found in .env file")

# ───────── CONFIGURATION ─────────
INPUT_FILE     = r"D:\CIR\prefect_ELT\Con_Edison_Orange_and_Rockland_qgis.parquet"  # path to GeoParquet
TABLE_NAME     = "distribution"
SCHEMA         = "public"
CHUNKSIZE      = 750_000   # rows per DB insert
BATCHSIZE      = 1_000_000   # rows read from parquet at a time

# Only keep these columns + geometry. Set to None to keep all.
SELECT_COLUMNS: Optional[List[str]] = [
    "id","feeder_id","substation","voltage_kv","hosting_capacity_kw","owner_name", "raw_data"
]

# DB engine
engine = create_engine(DB_URL)


# ───────── Helpers ─────────
def force_epsg4326(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Ensure EPSG:4326 for PostGIS upload."""
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326, allow_override=True)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    return gdf


def apply_select_columns(gdf: gpd.GeoDataFrame, selects: Optional[List[str]]) -> gpd.GeoDataFrame:
    if not selects:
        return gdf
    keep = [c for c in selects if c in gdf.columns]
    # Always keep geometry
    if gdf.geometry.name not in keep:
        keep.append(gdf.geometry.name)
    return gdf[keep]


from shapely import wkb

def stream_parquet_batches(parquet_file: str, batch_size: int):
    """Yield GeoDataFrames in batches directly from a big parquet file."""
    pf = pq.ParquetFile(parquet_file)
    for batch in pf.iter_batches(batch_size=batch_size):
        df = batch.to_pandas()

        # Detect geometry column automatically
        if "geometry" not in df.columns:
            raise ValueError("❌ No 'geometry' column found in parquet file")

        # Decode WKB → Shapely
        df["geom"] = df["geometry"].apply(lambda x: wkb.loads(x) if isinstance(x, (bytes, bytearray)) else x)
        df = df.drop(columns=["geometry"])  # drop old WKB column

        # Create GeoDataFrame with new 'geom' column
        gdf = gpd.GeoDataFrame(df, geometry="geom", crs="EPSG:4326")
        yield gdf



# ───────── Upload ─────────
def upload_parquet_to_postgis(parquet_file: str, batch_size: int = BATCHSIZE):
    if not os.path.exists(parquet_file):
        raise FileNotFoundError(f"❌ Input file not found: {parquet_file}")

    print(f"📂 Streaming from {parquet_file} in batches of {batch_size} rows...")
    first = True
    total_uploaded = 0

    for gdf in stream_parquet_batches(parquet_file, batch_size):
        if gdf.empty:
            continue

        # Normalize CRS and filter columns
        gdf = force_epsg4326(gdf)
        gdf = apply_select_columns(gdf, SELECT_COLUMNS)

        # Upload this batch
        print(f"📤 Uploading {len(gdf)} rows...")
        gdf.to_postgis(
            TABLE_NAME,
            engine,
            schema=SCHEMA,
            if_exists="replace" if first else "append",
            index=False,
            chunksize=CHUNKSIZE,
        )
        total_uploaded += len(gdf)
        first = False
        print(f"✅ Uploaded {total_uploaded} rows so far...")

    print(f"🎉 Done! Total uploaded: {total_uploaded} rows → {SCHEMA}.{TABLE_NAME}")


# ───────── MAIN ─────────
def main():
    upload_parquet_to_postgis(INPUT_FILE, BATCHSIZE)


if __name__ == "__main__":
    main()
