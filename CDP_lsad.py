# import geopandas as gpd
# import pandas as pd
# import os

# # ───────── CONFIGURATION ─────────
# INPUT_FOLDER = r"D:\CIR\TIGER2024_PLACE"
# OUTPUT_FOLDER = r"D:\CIR\TIGER2024\Summary"

# os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# # ───────── PROCESS EACH STATE FILE ─────────
# for file in os.listdir(INPUT_FOLDER):
#     if not file.endswith(".zip"):
#         continue
    
#     state_file = os.path.join(INPUT_FOLDER, file)
#     state_code = file.split("_")[2]   # e.g. tl_2024_06_place.zip → "06"
#     print(f"\n📂 Processing state {state_code} ({file})...")
    
#     # Load shapefile
#     gdf = gpd.read_file(state_file)
#     total = len(gdf)
#     print(f"   ✅ Total places: {total}")
    
#     # Verify LSAD ↔ MTFCC for CDPs
#     lsad57_not_g4210 = gdf[(gdf["LSAD"] == "57") & (gdf["MTFCC"] != "G4210")]
#     g4210_not_lsad57 = gdf[(gdf["MTFCC"] == "G4210") & (gdf["LSAD"] != "57")]
    
#     print(f"   ⚡ LSAD=57 but not G4210: {len(lsad57_not_g4210)}")
#     print(f"   ⚡ G4210 but not LSAD=57: {len(g4210_not_lsad57)}")
    
#     # Group by LSAD
#     lsad_counts = gdf.groupby("LSAD").size().reset_index(name="Count")
#     lsad_counts["Feature"] = "LSAD"
    
#     # Group by MTFCC
#     mtfcc_counts = gdf.groupby("MTFCC").size().reset_index(name="Count")
#     mtfcc_counts["Feature"] = "MTFCC"
    
#     # Combine into summary
#     summary_df = pd.concat([lsad_counts, mtfcc_counts], ignore_index=True)
    
#     # ───────── PRINT SUMMARY TO CONSOLE ─────────
#     print("   📊 Breakdown:")
#     for _, row in summary_df.iterrows():
#         print(f"      {row['Feature']} {row.iloc[0]} → {row['Count']}")
    
#     # ───────── SAVE TO EXCEL ─────────
#     output_excel = os.path.join(OUTPUT_FOLDER, f"Places_Summary_{state_code}.xlsx")
#     with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
#         summary_df.to_excel(writer, sheet_name="Summary", index=False)
        
#         # Save mismatches if any
#         if not lsad57_not_g4210.empty:
#             lsad57_not_g4210.drop(columns="geometry").to_excel(writer, sheet_name="LSAD57_not_G4210", index=False)
#         if not g4210_not_lsad57.empty:
#             g4210_not_lsad57.drop(columns="geometry").to_excel(writer, sheet_name="G4210_not_LSAD57", index=False)
        
#         # Save all places without geometry
#         gdf.drop(columns="geometry").to_excel(writer, sheet_name="All_Places", index=False)
    
#     print(f"   📂 Summary written to {output_excel}")


# import geopandas as gpd
# import pandas as pd
# import os

# # ───────── CONFIGURATION ─────────
# INPUT_FOLDER   = r"D:\CIR\TIGER2024_PLACE"
# OUTPUT_EXCEL   = r"D:\CIR\TIGER2024_PLACE\Places_Statewise_Summary.xlsx"

# # Collect results
# summary_results = []

# # ───────── PROCESS ALL STATES ─────────
# for file in os.listdir(INPUT_FOLDER):
#     if file.endswith(".zip"):
#         state_file = os.path.join(INPUT_FOLDER, file)
#         print(f"Loading {file} ...")

#         # Load shapefile
#         gdf = gpd.read_file(state_file)
#         statefp = gdf["STATEFP"].iloc[0] if "STATEFP" in gdf.columns else os.path.splitext(file)[0]
#         print(f"✅ Loaded {len(gdf)} places for STATEFP={statefp}")

#         # ---- Counts ----
#         lsad_counts = gdf["LSAD"].value_counts().to_dict()
#         mtfcc_counts = gdf["MTFCC"].value_counts().to_dict()

#         total_places = len(gdf)
#         total_cdps = (gdf["LSAD"] == "57").sum()
#         total_non_cdps = total_places - total_cdps

#         # Collect state summary
#         summary = {
#             "STATEFP": statefp,
#             "Total_Places": total_places,
#             "Total_CDPs": total_cdps,
#             "Total_NonCDPs": total_non_cdps,
#         }

#         # Add LSAD and MTFCC counts
#         for k, v in lsad_counts.items():
#             summary[f"LSAD_{k}"] = v
#         for k, v in mtfcc_counts.items():
#             summary[f"MTFCC_{k}"] = v

#         summary_results.append(summary)

# # ───────── CREATE DATAFRAME ─────────
# summary_df = pd.DataFrame(summary_results)

# # Fill missing LSAD/MTFCC columns with 0
# summary_df = summary_df.fillna(0).astype({col: int for col in summary_df.columns if col != "STATEFP"})

# # ───────── SAVE TO EXCEL ─────────
# summary_df.to_excel(OUTPUT_EXCEL, index=False, engine="openpyxl")

# print(f"📂 Final statewise summary saved to {OUTPUT_EXCEL}")


import geopandas as gpd
import pandas as pd
import os

# ───────── CONFIGURATION ─────────
INPUT_FOLDER   = r"D:\CIR\TIGER2024_PLACE"
OUTPUT_EXCEL   = r"D:\CIR\TIGER2024_PLACE\Places_Statewise_Summary.xlsx"

# Collect results
summary_results = []

# ───────── PROCESS ALL STATES ─────────
for file in os.listdir(INPUT_FOLDER):
    if file.endswith(".zip"):
        state_file = os.path.join(INPUT_FOLDER, file)
        print(f"Loading {file} ...")

        # Load shapefile
        gdf = gpd.read_file(state_file)
        statefp = gdf["STATEFP"].iloc[0] if "STATEFP" in gdf.columns else os.path.splitext(file)[0]
        print(f"✅ Loaded {len(gdf)} places for STATEFP={statefp}")

        # ---- Counts ----
        lsad_counts = gdf["LSAD"].value_counts().to_dict()
        mtfcc_counts = gdf["MTFCC"].value_counts().to_dict()

        total_places = len(gdf)
        total_cdps = (gdf["LSAD"] == "57").sum()
        total_non_cdps = total_places - total_cdps

        # Count CDPs by name (NAMELSAD contains "CDP")
        name_cdp_count = gdf["NAMELSAD"].str.contains("CDP", case=False, na=False).sum()

        # Collect state summary
        summary = {
            "STATEFP": statefp,
            "Total_Places": total_places,
            "Total_CDPs": total_cdps,
            "Total_NonCDPs": total_non_cdps,
            "Name_CDP_Count": name_cdp_count,
        }

        # Add LSAD and MTFCC counts
        for k, v in lsad_counts.items():
            summary[f"LSAD_{k}"] = v
        for k, v in mtfcc_counts.items():
            summary[f"MTFCC_{k}"] = v

        summary_results.append(summary)

# ───────── CREATE DATAFRAME ─────────
summary_df = pd.DataFrame(summary_results)

# Fill missing LSAD/MTFCC columns with 0
for col in summary_df.columns:
    if col not in ["STATEFP"]:
        summary_df[col] = summary_df[col].fillna(0).astype(int)

# ───────── ADD U.S. TOTAL ROW ─────────
us_total = summary_df.drop(columns=["STATEFP"]).sum(numeric_only=True)
us_total["STATEFP"] = "US_Total"
summary_df = pd.concat([summary_df, pd.DataFrame([us_total])], ignore_index=True)

# ───────── SAVE TO EXCEL ─────────
summary_df.to_excel(OUTPUT_EXCEL, index=False, engine="openpyxl")

print(f"📂 Final statewise summary with Name_CDP_Count saved to {OUTPUT_EXCEL}")
