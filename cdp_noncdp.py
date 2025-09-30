import geopandas as gpd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# ───────── LOAD ENV FILE ─────────
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

if not DB_URL:
    raise ValueError("❌ DATABASE_URL not found in .env file")

# ───────── CONFIGURATION ─────────
INPUT_FOLDER   = r"D:\CIR\TIGER2024_PLACE"                # folder with .zip shapefiles
PARQUET_FOLDER = os.path.join(INPUT_FOLDER, "parquet_states")  # output parquet files
TABLE_NAME     = "us_places"
SCHEMA         = "public"
CHUNKSIZE      = 50000

# Ensure parquet folder exists
os.makedirs(PARQUET_FOLDER, exist_ok=True)

# Create DB engine
engine = create_engine(DB_URL)

# ───────── FUNCTION: normalize column names ─────────
def normalize_columns(df):
    df.columns = (
        df.columns.str.strip()
                  .str.lower()
                  .str.replace(" ", "_")
                  .str.replace("-", "_")
    )
    return df

# ───────── STEP 1: Convert Shapefiles → GeoParquet ─────────
print("🔄 Converting shapefiles to GeoParquet...")
parquet_files = []

for file in os.listdir(INPUT_FOLDER):
    if file.endswith(".zip"):
        state_file = os.path.join(INPUT_FOLDER, file)
        state_name = os.path.splitext(file)[0]
        parquet_file = os.path.join(PARQUET_FOLDER, f"{state_name}.parquet")

        print(f"📂 Reading {file} ...")
        gdf = gpd.read_file(state_file)

        # Normalize CRS to EPSG:4326
        if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)

        # Normalize column names
        gdf = normalize_columns(gdf)

        # Save to GeoParquet
        gdf.to_parquet(parquet_file, index=False)
        parquet_files.append(parquet_file)
        print(f"✅ Saved {len(gdf)} rows to {parquet_file}")

print(f"🎉 Finished converting {len(parquet_files)} states to GeoParquet with crs 4326.")

# ───────── STEP 2: Upload GeoParquet → PostGIS ─────────
print("🚀 Uploading GeoParquet files to PostGIS...")

first = True  # first file will replace, others append

for parquet_file in parquet_files:
    print(f"📂 Uploading {os.path.basename(parquet_file)} ...")
    gdf = gpd.read_parquet(parquet_file)

    gdf.to_postgis(
        TABLE_NAME,
        engine,
        schema=SCHEMA,
        if_exists="replace" if first else "append",
        index=False,
        chunksize=CHUNKSIZE
    )

    print(f"✅ Uploaded {len(gdf)} rows from {os.path.basename(parquet_file)}")
    first = False

print(f"🎉 All data uploaded to {SCHEMA}.{TABLE_NAME} in PostGIS")