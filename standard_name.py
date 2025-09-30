
import os
import re
import pandas as pd
import geopandas as gpd
import json
import requests
from sqlalchemy import create_engine
from geoalchemy2 import Geometry
from sqlalchemy.dialects.postgresql import JSONB




def looks_empty(x) -> bool:
    if x is None:
        return True
    s = str(x).strip().lower()
    return s in ("", "nan", "none", "null", "na", "n/a")

def normalize(s: str) -> str:
    """Normalize names for fuzzy matching."""
    s = str(s).lower()
    s = s.replace("&", "and").replace("_", " ")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def build_operator_mappings(csv_file, target_fields=None):
    """
    Reads Excel mapping and builds dictionary:
    { operator_name (from row 'owner_name') : { standard_field : operator_field_or_literal } }
    
    Only includes rows listed in target_fields (if provided).
    """
    df = pd.read_csv(csv_file)
    standard_col = df.columns[0]

    # Row with operator names
    owner_row = df[df[standard_col].str.lower() == "owner_name"].iloc[0]

    operator_mappings = {}

    for col in df.columns[1:]:
        operator_name = str(owner_row[col]).strip()
        if looks_empty(operator_name):
            continue

        op_map = {}
        for _, row in df.iterrows():
            std_name = str(row[standard_col]).strip()
            op_val = row[col]
            if (
                looks_empty(op_val)
                or looks_empty(std_name)
                or std_name.lower() == "owner_name"
            ):
                continue

            # Only include explicitly chosen fields
            if target_fields and std_name not in target_fields:
                continue

            op_map[std_name] = str(op_val).strip()

        operator_mappings[operator_name] = op_map

    return operator_mappings

def match_operator(folder_name: str, operator_mappings: dict) -> str | None:
    """Find best matching operator key from Excel for a folder name."""
    f = normalize(folder_name)
    best = None
    best_score = 0
    for op_name in operator_mappings.keys():
        o = normalize(op_name)
        if not o:
            continue
        if o in f or f in o:  # substring match
            score = len(o)
            if score > best_score:
                best_score = score
                best = op_name
    return best

def fetch_alias_map_from_excel(df, operator_name: str) -> dict:
    """Fetch alias map for an operator using alias link from row 2 in Excel."""
    standard_col = df.columns[0]
    owner_row = df[df[standard_col].str.lower() == "owner_name"].iloc[0]

    # Find operator column
    op_col = None
    for col in df.columns[1:]:
        if str(owner_row[col]).strip().lower() == operator_name.strip().lower():
            op_col = col
            break
    if not op_col:
        return {}

    alias_link = str(df[op_col].iloc[0]).strip()
    if looks_empty(alias_link):
        return {}

    try:
        resp = requests.get(alias_link, params={"f": "json"}, timeout=20)
        if not resp.ok:
            print(f"⚠️ Failed alias fetch for {operator_name} (HTTP {resp.status_code})")
            return {}
        data = resp.json()
        return {fld["name"]: fld.get("alias", fld["name"]) for fld in data.get("fields", []) if "name" in fld}
    except Exception as e:
        print(f"⚠️ Error fetching alias map for {operator_name}: {e}")
        return {}


def apply_mapping_to_geojson(
    geojson_file, mapping, operator_name,
    alias_map=None, output_file=None, drop_fields=None
):
    if drop_fields is None:
        drop_fields = [
            "gridforce_feeder_url", "gridforce_substation_url", "GlobalID", "DATE_UPDATED", "DATE_CUTOF",
            "OBJECTID", "OBJECTID_1", "objectid", "shape_Length", "shape_Area", "notes"
            "shape__length", "shape__area","Shape__Length", "Shape__Area",
            "color", "Color", "Color__", "color__", "colour", "LAST PopupInfo", "Last PopupInfo", "LAST_PopupInfo", "LoadCurve",  "SystemData" ]

    gdf = gpd.read_file(geojson_file)
    gdf["owner_name"] = operator_name
    raw_records = []

    # --- Build standardized + raw_data ---
    for idx, row in gdf.iterrows():
        raw_entry = {}

        for col, val in row.items():
            if col == gdf.geometry.name:  # skip geometry
                continue
            if col in drop_fields:  # skip dropped fields
                continue

            # 1) Check if mapped to standard field
            std_field = next((k for k, v in mapping.items() if v == col), None)

            if std_field:
                gdf.at[idx, std_field] = val
            else:
                # 2) Otherwise → alias → raw_data
                alias_name = alias_map.get(col, col) if alias_map else col
                if not looks_empty(val):
                    try:
                        json.dumps(val)  # test serializable
                        raw_entry[alias_name] = val
                    except TypeError:
                        raw_entry[alias_name] = str(val)

        raw_records.append(json.dumps(raw_entry))

    gdf["raw_data"] = raw_records

    # --- Keep only final schema ---
    final_cols = list(mapping.keys()) + ["owner_name", gdf.geometry.name, "raw_data"]
    gdf = gdf[final_cols]

    # --- Save + Upload ---
    if output_file:
        gdf.to_file(output_file, driver="GeoJSON")
        print(f"✅ Saved {output_file}")
        CONN_STR = "postgresql://DDR_DB:santhoshkumar@cir-ddr-tool-db.czsmk6a6ycrk.ap-south-1.rds.amazonaws.com:5432/postgres"
        upload_to_postgis(gdf, "us_distribution_lines_test", CONN_STR, "append")
    else:
        gdf.to_file(geojson_file, driver="GeoJSON")
        print(f"✅ Overwritten {geojson_file}")
        CONN_STR = "postgresql://DDR_DB:santhoshkumar@cir-ddr-tool-db.czsmk6a6ycrk.ap-south-1.rds.amazonaws.com:5432/postgres"
        upload_to_postgis(gdf, "us_distribution_lines_test", CONN_STR, "append")




def process_state(csv_file, geojson_root, state_folder, overwrite, target_fields):
    state_name = os.path.basename(state_folder)
    print(f"\n=== Processing {state_name} ===")
    operator_mappings = build_operator_mappings(csv_file, target_fields=target_fields)

    if not os.path.exists(state_folder):
        print(f"❌ State folder not found: {state_folder}")
        return

    for folder in os.listdir(state_folder):
        operator_folder = os.path.join(state_folder, folder)
        if not os.path.isdir(operator_folder):
            print(f"➡️ Found operator folder: {operator_folder}")
            continue
        print(f"   🔍 Found operator folder: {operator_folder}")
        print(f"   🔍 Matching operator for folder: {folder}")
        best_op = match_operator(folder, operator_mappings)
        if not best_op:
            print(f"⚠️ No mapping found for folder '{folder}'")
            continue

        mapping = operator_mappings[best_op]
        print(f"\nApplying mapping for folder '{folder}' → Excel operator '{best_op}'")
        print(f"Fields being standardized: {list(mapping.keys())}")

        for file in os.listdir(operator_folder):
            if file.endswith(".geojson"):
                print(f"   📂 Processing file: {file}")   # <-- add this
                in_file = os.path.join(operator_folder, file)
                if overwrite:
                    out_file = None
                else:
                    out_file = in_file.replace(".geojson", "_standard.geojson")
                
                df = pd.read_csv(csv_file)
                alias_map = fetch_alias_map_from_excel(df, best_op)
                apply_mapping_to_geojson(in_file, mapping, best_op, alias_map, out_file)
               

def upload_to_postgis(gdf, table_name, conn_str, if_exists="append"):
    """
    Upload a GeoDataFrame to PostGIS with only owner_name, raw_data, and geometry.
    
    Parameters
    ----------
    gdf : GeoDataFrame
        The GeoDataFrame containing processed data.
    table_name : str
        Destination PostGIS table name.
    conn_str : str
        SQLAlchemy connection string, e.g.
        "postgresql+psycopg2://user:password@host:5432/dbname"
    if_exists : str
        "replace", "append", or "fail"
    """

    # Keep only required columns
    gdf = gdf[["owner_name", "raw_data", gdf.geometry.name]]

    # Ensure CRS is EPSG:4326 before upload
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")

    # Connect
    engine = create_engine(conn_str)

    # Upload
    gdf.to_postgis(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
        dtype={"geometry": Geometry("GEOMETRY", srid=4326)}
    )

    print(f"✅ Uploaded {len(gdf)} features into {table_name}")
# -----------------------------
# Example usage
# -----------------------------
if __name__ == "__main__":
    csv_folder = r"D:\CIR\statewise_excel"
    geojson_root = r"D:\CIR\state_test"

    # Explicitly say which fields to standardize
    fields_to_standardize = ["owner_name"]

    for state_folder in os.listdir(geojson_root):
        state_name = os.path.join(geojson_root, state_folder)
        if not os.path.isdir(state_name):
            continue

        norm = normalize(state_folder)
        print(f"Checking {norm}")
        csv_match = None
        for csv_file in os.listdir(csv_folder):
            print(f"Checking {csv_file}")
            if csv_file.lower().endswith(".csv") and normalize(os.path.splitext(csv_file)[0]) == norm:
                csv_match = os.path.join(csv_folder, csv_file)
                print(f"✅ CSV found for {csv_match}")
                break
        
        if not csv_match:
            print(f"❌ No CSV found for {state_folder} {csv_match}")
            continue
        
        process_state(csv_match, geojson_root, state_name,overwrite=False, target_fields=fields_to_standardize)

