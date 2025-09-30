import geopandas as gpd
from pathlib import Path

INPUT_ROOT = r"D:\CIR\state"
OUTPUT_ROOT = r"D:\CIR\state_geoparquet"

def fix_and_convert():
    input_root = Path(INPUT_ROOT)
    output_root = Path(OUTPUT_ROOT)
    output_root.mkdir(parents=True, exist_ok=True)
    
    total_files = 0
    success_count = 0

    for geojson_path in input_root.rglob("*.geojson"):
        total_files += 1
        try:
            print(f"🔧 Processing: {geojson_path.relative_to(input_root)}")
            
            gdf = gpd.read_file(geojson_path)
            if gdf.empty:
                print("   ⚠️ Skipping empty file")
                continue

            # ✅ CORRECT: Call make_valid() on the GeoDataFrame
            gdf_fixed = gdf.make_valid()

            gdf_fixed.to_file(geojson_path, driver="GeoJSON")
            print(f"✅ Saved {geojson_path}")
            success_count += 1
            
        except KeyboardInterrupt:
            print("\n🛑 Stopped by user.")
            break
        except Exception as e:
            print(f"   ❌ ERROR in {geojson_path.name}: {e}")

    print(f"\n✨ Completed! {success_count}/{total_files} files processed.")

if __name__ == "__main__":
    fix_and_convert()
