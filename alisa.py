import pandas as pd
import requests

def looks_empty(x) -> bool:
    if x is None:
        return True
    s = str(x).strip().lower()
    return s in ("", "nan", "none", "null", "na", "n/a")

def fetch_alias_map(alias_link: str) -> dict:
    """Fetch alias map from ArcGIS service."""
    alias_map = {}
    if looks_empty(alias_link):
        return alias_map

    try:
        resp = requests.get(alias_link.strip(), params={"f": "json"}, timeout=20)
        if not resp.ok:
            print(f"⚠️ Failed to fetch {alias_link} (HTTP {resp.status_code})")
            return alias_map
        data = resp.json()
        print(data)
        alias_map = {fld["name"]: fld.get("alias", fld["name"]) for fld in data.get("fields", []) if "name" in fld}
    except Exception as e:
        print(f"⚠️ Error fetching {alias_link}: {e}")
    return alias_map

def test_aliases(csv_file: str):
    df = pd.read_csv(csv_file)
    standard_col = df.columns[0]

    # Row with operator names
    owner_row = df[df[standard_col].str.lower() == "owner_name"].iloc[0]

    print("\n=== Testing alias fetching ===\n")

    for col in df.columns[1:]:
        operator_name = str(owner_row[col]).strip()
        alias_link = str(df[col].iloc[0]).strip()  # row 2 (index 0 since header is row 1)

        if looks_empty(operator_name):
            continue

        print(f"\nOperator: {operator_name}")
        print(f"Alias link: {alias_link}")

        alias_map = fetch_alias_map(alias_link)
        if alias_map:
            print("Fetched fields and aliases:")
            for field, alias in alias_map.items():
                print(f"  {field}  →  {alias}")
        else:
            print("⚠️ No alias map fetched.")

if __name__ == "__main__":
    # Change this path to your CSV
    csv_file = r"D:\CIR\statewise_excel\New_York.csv"
    test_aliases(csv_file)
