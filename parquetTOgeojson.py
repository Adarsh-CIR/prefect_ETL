import geopandas as gpd
from pathlib import Path

# --- CONFIG ---
# Set your input GeoParquet path here
input_file = r"D:\CIR\state_test\Hawaii\Hawaii Island\Hawaii_Island__latest_20250922_171311.parquet"
# input_file = r"D:\CIR\state_test\Hawaii\MAUI\Hawaii_Maui__latest_20250922_170140.parquet"

# Optional: specify output file (otherwise same name with .geojson)
output_file = None  

# --- CONVERSION FUNCTION ---
def geoparquet_to_geojson(input_file, output_file=None):
    """
    Convert a GeoParquet file to GeoJSON.
    """
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    if output_file is None:
        output_file = input_path.with_suffix(".geojson")
    
    print(f"Reading {input_file}...")
    gdf = gpd.read_parquet(input_file)

    print(f"Writing to {output_file}...")
    gdf.to_file(output_file, driver="GeoJSON")

    print("✅ Conversion completed!")

# --- RUN ---
geoparquet_to_geojson(input_file, output_file)
