import os
import pyarrow.dataset as ds
import geopandas as gpd
from shapely import wkb, wkt
from shapely.geometry import MultiLineString, LineString
from sqlalchemy import create_engine
from geoalchemy2 import Geometry
import dotenv

dotenv.load_dotenv()

# ======================
# CONFIG
# ======================
INPUT_FILE = r"D:\CIR\prefect_ELT\merged_roads.parquet"
TABLE_NAME = "us_roads"
CHUNK_SIZE = 50_000   # adjust based on RAM
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL not set in .env")

# ======================
# CONNECT
# ======================
engine = create_engine(DATABASE_URL)

# ======================
# READ DATASET IN CHUNKS
# ======================
dataset = ds.dataset(INPUT_FILE, format="parquet")

# Only keep required columns
columns = ["FULLNAME", "geometry", "PRETYPEABRV"]

print("Starting upload...")

batch_num = 0
for batch in dataset.to_batches(columns=columns, batch_size=CHUNK_SIZE):
    batch_num += 1
    print(f"Processing batch {batch_num}...")

    # Convert Arrow batch → pandas DataFrame
    df = batch.to_pandas()

    # Try to interpret geometry
    if df["geometry"].dtype == object:  
        try:
            # Try WKB
            geom_series = df["geometry"].apply(lambda g: wkb.loads(g) if g is not None else None)
        except Exception:
            # If WKT fallback
            geom_series = df["geometry"].apply(lambda g: wkt.loads(g) if g is not None else None)
    else:
        # Already geometry
        geom_series = df["geometry"]

    # Build GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=geom_series)

    # Set CRS if missing
    if gdf.crs is None:
        gdf.set_crs("EPSG:4326", inplace=True)

    # Ensure all are MULTILINESTRING
    gdf["geometry"] = gdf["geometry"].apply(
        lambda g: g if g is None or g.geom_type == "MultiLineString" 
        else MultiLineString([g]) if isinstance(g, LineString) 
        else g
    )

    # ======================
    # UPLOAD TO POSTGIS
    # ======================
    gdf.to_postgis(
        TABLE_NAME,
        engine,
        if_exists="append",   # append chunks
        index=False,
        dtype={"geometry": Geometry("MULTILINESTRING", srid=4326)}
    )

print("✅ Upload completed successfully!")
