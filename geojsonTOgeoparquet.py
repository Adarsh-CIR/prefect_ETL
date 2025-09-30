import fiona
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import shape, mapping
import shapely.wkb as swkb

input_file = r"D:\CIR\prefect_ELT\Secondary_Roads_Interstates_and_US_Highways.geojson"
output_file = r"D:\CIR\prefect_ELT\Secondary_Roads_Interstates_and_US_Highways.parquet"

# Open GeoJSON in streaming mode
with fiona.open(input_file, "r") as src:
    schema = src.schema
    crs = src.crs

    # Convert CRS to EPSG if needed
    epsg = None
    if crs and "init" in crs:
        epsg = crs["init"].upper().replace("EPSG:", "")

    batch_size = 50000  # adjust depending on memory
    features = []
    table_batches = []

    for i, feat in enumerate(src):
        geom = shape(feat["geometry"])
        # store geometry as WKB (compact binary form)
        geom_wkb = swkb.dumps(geom)

        row = {**feat["properties"], "geometry": geom_wkb}
        features.append(row)

        if len(features) >= batch_size:
            # Convert to Arrow Table
            batch = pa.Table.from_pylist(features)
            table_batches.append(batch)
            features = []

    # Last batch
    if features:
        batch = pa.Table.from_pylist(features)
        table_batches.append(batch)

# Write all batches into a single parquet file
pq.write_table(pa.concat_tables(table_batches), output_file)

print(f"✅ Saved GeoParquet: {output_file}")
