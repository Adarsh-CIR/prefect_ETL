# import pandas as pd
# import geopandas as gpd
# import requests
# import json
# import os
# from sqlalchemy import create_engine

# def process_state(state_name, csv_folder, geojson_root, engine):
#     # 1. Locate CSV
#     csv_file = os.path.join(csv_folder, f"{state_name}.csv")
#     if not os.path.exists(csv_file):
#         print(f"❌ Mapping CSV not found for {state_name}")
#         return
    
#     mapping_df = pd.read_csv(csv_file)
#     alias_link = mapping_df.iloc[1, 0]  # row 2 col A
    
#     # Build operator mapping {operator_name: {operator_field: standard_field/None}}
#     operator_mappings = {}
#     standard_col = mapping_df.columns[0]
#     for col in mapping_df.columns[1:]:
#         operator_mappings[col.strip()] = {}
#         for _, row in mapping_df.iterrows():
#             std = row[standard_col]
#             alias = row[col]
#             if pd.notna(alias):
#                 if pd.notna(std):
#                     operator_mappings[col.strip()][alias] = std
#                 else:
#                     operator_mappings[col.strip()][alias] = None
    
#     # 2. Fetch alias map from API
#     alias_map = {}
#     try:
#         resp = requests.get(alias_link, params={"f": "json"}, timeout=15)
#         if resp.ok:
#             data = resp.json()
#             for f in data.get("fields", []):
#                 alias_map[f["name"]] = f.get("alias", f["name"])
#     except Exception as e:
#         print(f"⚠️ Could not fetch alias map for {state_name}: {e}")
    
#     # 3. Process operator folders inside this state
#     state_folder = os.path.join(geojson_root, state_name.replace(" ", "_"))
#     if not os.path.exists(state_folder):
#         print(f"❌ GeoJSON folder not found for {state_name}")
#         return
    
#     for operator in os.listdir(state_folder):
#         operator_folder = os.path.join(state_folder, operator)
#         if not os.path.isdir(operator_folder):
#             continue
        
#         mapping = operator_mappings.get(operator.strip(), {})
#         if not mapping:
#             print(f"⚠️ No mapping found for operator {operator} in {state_name}")
#             continue
        
#         # Process each GeoJSON file
#         for file in os.listdir(operator_folder):
#             if not file.endswith(".geojson"):
#                 continue
#             file_path = os.path.join(operator_folder, file)
#             gdf = gpd.read_file(file_path)
            
#             final_rows = []
#             for _, row in gdf.iterrows():
#                 record = {}
#                 raw_data = {}
#                 for col, val in row.items():
#                     if col in mapping:
#                         if mapping[col]:
#                             record[mapping[col]] = val
#                         else:
#                             raw_data[alias_map.get(col, col)] = val
#                     else:
#                         raw_data[alias_map.get(col, col)] = val
#                 record["raw_data"] = json.dumps(raw_data)
#                 final_rows.append(record)
            
#             final_gdf = gpd.GeoDataFrame(final_rows, geometry=gdf.geometry)
#             final_gdf.to_postgis("distribution_lines", engine, if_exists="append", index=False)
#             print(f"✅ Loaded {file} ({operator}, {state_name}) into PostGIS")

# # Example usage
# engine = create_engine("postgresql://postgres:postgres@localhost:5432/postgres")

# csv_folder = r"D:\CIR\statewise_excel"
# geojson_root = r"D:\CIR\statewise_distri\New_York"

# process_state("New_York", csv_folder, geojson_root, engine)


# import pandas as pd
# import geopandas as gpd
# import requests
# import json
# import os
# from sqlalchemy import create_engine

# def process_state(state_name, csv_folder, geojson_root, engine):
#     # 1. Locate CSV
#     csv_file = os.path.join(csv_folder, f"{state_name}.csv")
#     if not os.path.exists(csv_file):
#         print(f"❌ Mapping CSV not found for {state_name}")
#         return
    
#     mapping_df = pd.read_csv(csv_file)
#     standard_col = mapping_df.columns[0]

#     # Row 2 (index 0) contains alias links per operator (skip col A)
#     alias_links = mapping_df.iloc[0, 1:].to_dict()

#     # Build operator mapping {operator_name: {operator_field: standard_field/None}}
#     operator_mappings = {}
#     for col in mapping_df.columns[1:]:
#         operator_mappings[col.strip()] = {}
#         for _, row in mapping_df.iloc[1:].iterrows():  # skip alias row
#             std = row[standard_col]
#             alias = row[col]
#             if pd.notna(alias):
#                 if pd.notna(std):
#                     operator_mappings[col.strip()][alias] = std
#                 else:
#                     operator_mappings[col.strip()][alias] = None
    
#     # 2. Process operator folders inside this state
#     if not os.path.exists(geojson_root):
#         print(f"❌ GeoJSON folder not found for {state_name}")
#         return
    
#     for operator in os.listdir(geojson_root):
#         operator_folder = os.path.join(geojson_root, operator)
#         if not os.path.isdir(operator_folder):
#             continue
        
#         mapping = operator_mappings.get(operator.strip(), {})
#         alias_link = alias_links.get(operator, None)

#         # Fetch alias map for this operator
#         alias_map = {}
#         if alias_link and str(alias_link).lower() != "nan":
#             try:
#                 resp = requests.get(str(alias_link), params={"f": "json"}, timeout=15)
#                 if resp.ok:
#                     data = resp.json()
#                     for f in data.get("fields", []):
#                         alias_map[f["name"]] = f.get("alias", f["name"])
#             except Exception as e:
#                 print(f"⚠️ Could not fetch alias map for {operator} ({state_name}): {e}")
        
#         if not mapping:
#             print(f"⚠️ No mapping found for operator {operator} in {state_name}")
#             continue
        
#         # Process each GeoJSON file
#         for file in os.listdir(operator_folder):
#             if not file.endswith(".geojson"):
#                 continue
#             file_path = os.path.join(operator_folder, file)
#             gdf = gpd.read_file(file_path)
            
#             final_rows = []
#             for _, row in gdf.iterrows():
#                 record = {}
#                 raw_data = {}
#                 for col, val in row.items():
#                     if col in mapping:
#                         if mapping[col]:
#                             record[mapping[col]] = val
#                         else:
#                             raw_data[alias_map.get(col, col)] = val
#                     else:
#                         raw_data[alias_map.get(col, col)] = val
#                 record["raw_data"] = json.dumps(raw_data)
#                 final_rows.append(record)
            
#             final_gdf = gpd.GeoDataFrame(final_rows, geometry=gdf.geometry)
#             final_gdf.to_postgis("distribution_lines", engine, if_exists="append", index=False)
#             print(f"✅ Loaded {file} ({operator}, {state_name}) into PostGIS")

# # Example usage
# engine = create_engine("postgresql://postgres:postgres@localhost:5432/postgres")

# csv_folder = r"D:\CIR\statewise_excel"
# geojson_root = r"D:\CIR\statewise_distri\New_York"

# process_state("New_York", csv_folder, geojson_root, engine)

import os
import re
import json
import pandas as pd
import geopandas as gpd
import requests
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB


# -----------------------------
# Helpers
# -----------------------------
def norm_name(s: str) -> str:
    """Normalize names for comparison."""
    if s is None:
        return ""
    s = str(s).lower()
    s = s.replace("&", "and").replace("_", " ")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def looks_empty(x) -> bool:
    if x is None:
        return True
    s = str(x).strip().lower()
    return s in ("", "nan", "none", "null", "na", "n/a")

def parse_operator_field(cell_val: str) -> str:
    """Extract key before ':' if present."""
    if looks_empty(cell_val):
        return ""
    s = str(cell_val)
    parts = s.split(":", 1)
    return parts[0].strip()

def find_best_operator_column(folder_op_name: str, operator_names: list[str]) -> str | None:
    """
    Keyword-based match: find best operator name from CSV that appears in folder name.
    """
    f = norm_name(folder_op_name)
    best = None
    best_score = 0
    for human_name in operator_names:
        h = norm_name(human_name)
        if not h:
            continue
        if h in f or f in h:
            score = len(h)
            if score > best_score:
                best_score = score
                best = human_name
    return best

def fetch_alias_map(url: str) -> dict:
    """Fetch alias map from ArcGIS service."""
    m = {}
    if looks_empty(url):
        return m
    try:
        resp = requests.get(url, params={"f": "json"}, timeout=25)
        if resp.ok:
            data = resp.json()
            for fld in data.get("fields", []):
                name = fld.get("name")
                alias = fld.get("alias") or name
                if name:
                    m[name] = alias
    except Exception:
        pass
    return m


# -----------------------------
# Core per-state processor
# -----------------------------
def process_state(csv_file: str, geojson_root: str, engine):
    state_name = os.path.splitext(os.path.basename(csv_file))[0]  # e.g., "New_York"
    print(f"\n=== Processing {state_name} ===")

    # Load CSV
    try:
        raw_df = pd.read_csv(csv_file, header=0)
    except Exception as e:
        print(f"❌ Failed to read CSV {csv_file}: {e}")
        return

    if raw_df.shape[0] < 4 or raw_df.shape[1] < 2:
        print(f"❌ CSV structure invalid for {csv_file}")
        return

    standard_col = raw_df.columns[0]

    # Row 2 → alias links
    alias_links = {}
    for col in raw_df.columns[1:]:
        link = str(raw_df[col].iloc[0]).strip()
        if not looks_empty(link):
            alias_links[col] = link

    # Row 4 → operator names
    operator_names_row = raw_df.iloc[2, 1:].to_dict()
    operator_headers = [standard_col] + [operator_names_row.get(col, col) for col in raw_df.columns[1:]]

    # Drop first 4 rows → start mapping at row 5
    df = raw_df.iloc[3:].reset_index(drop=True)
    df.columns = operator_headers

    # Build operator mappings
    operator_mappings = {}
    for human_name in df.columns[1:]:
        mapping = {}
        for _, row in df.iterrows():
            std_name = row[standard_col]
            op_cell = row[human_name]
            if looks_empty(op_cell):
                continue
            op_field = parse_operator_field(op_cell)
            if looks_empty(op_field):
                continue
            if not looks_empty(std_name):
                mapping[op_field] = str(std_name).strip()
            else:
                mapping[op_field] = None
        operator_mappings[human_name] = mapping

    # Map operator → alias link
    human_to_link = {}
    raw_cols = list(raw_df.columns)
    for i, col in enumerate(raw_cols[1:], start=1):
        human_name = operator_names_row.get(col)
        if looks_empty(human_name):
            continue
        link = alias_links.get(col, "")
        human_to_link[human_name] = link

    # Locate GeoJSON folder
    state_folder = os.path.join(geojson_root, state_name)
    if not os.path.exists(state_folder):
        print(f"❌ GeoJSON folder not found for {state_name} → expected {state_folder}")
        return

    csv_operator_names = list(operator_mappings.keys())

    # Process operator folders
    for operator_folder_name in os.listdir(state_folder):
        operator_folder = os.path.join(state_folder, operator_folder_name)
        if not os.path.isdir(operator_folder):
            continue

        best_csv_human = find_best_operator_column(operator_folder_name, csv_operator_names)
        if not best_csv_human:
            print(f"⚠️ No keyword match for folder '{operator_folder_name}' in {state_name}")
            continue

        mapping = operator_mappings.get(best_csv_human, {})
        if not mapping:
            print(f"⚠️ Mapping empty for '{best_csv_human}' (folder '{operator_folder_name}')")
            continue

        alias_link = human_to_link.get(best_csv_human, "")
        alias_map = fetch_alias_map(alias_link) if not looks_empty(alias_link) else {}

        # Process files
        for fname in os.listdir(operator_folder):
            if not fname.lower().endswith(".geojson"):
                continue
            fpath = os.path.join(operator_folder, fname)
            try:
                gdf = gpd.read_file(fpath)
            except Exception as e:
                print(f"⚠️ Skipping {fpath}: {e}")
                continue

            std_rows = []
            geom = gdf.geometry
            value_cols = [c for c in gdf.columns if c != gdf.geometry.name]

            for _, feat in gdf[value_cols].iterrows():
                record, raw = {}, {}
                for col in value_cols:
                    val = feat[col]
                    if looks_empty(val):
                        continue
                    if col in mapping:
                        std_key = mapping[col]
                        if std_key:
                            record[std_key] = val
                        else:
                            raw[alias_map.get(col, col)] = val
                    else:
                        raw[alias_map.get(col, col)] = val
                if raw:
                    record["raw_data"] = json.dumps(raw)
                std_rows.append(record)

            if not std_rows:
                print(f"ℹ️ No attributes in {fname}")
                continue

            final_gdf = gpd.GeoDataFrame(std_rows, geometry=geom)
            try:
                final_gdf = final_gdf.rename_geometry("geom")
            except Exception:
                final_gdf = final_gdf.set_geometry(geom)
                if "geometry" in final_gdf.columns:
                    final_gdf = final_gdf.rename(columns={"geometry": "geom"})
                    final_gdf.set_geometry("geom", inplace=True)

            try:
                final_gdf.to_postgis(
                    "distribution",
                    engine,
                    if_exists="append",
                    index=False,
                    dtype={"raw_data": JSONB}
                )
                print(f"✅ Loaded {fname} ({operator_folder_name} → {best_csv_human}, {state_name})")
                print(f"✅ Uploaded {len(final_gdf)} rows to {SCHEMA}.{TABLE_NAME}")
            except Exception as e:
                print(f"❌ Failed to load {fname}: {e}")


def main():
    csv_folder = r"D:\CIR\statewise_excel"
    geojson_root = r"D:\CIR\statewise_distri"
    db_url = "postgresql://postgres:secret@localhost:5432/postgres"  # update creds
    engine = create_engine(db_url)

    for file in os.listdir(csv_folder):
        if file.lower().endswith(".csv"):
            csv_path = os.path.join(csv_folder, file)
            process_state(csv_path, geojson_root, engine)


if __name__ == "__main__":
    main()
