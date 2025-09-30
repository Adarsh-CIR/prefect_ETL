import duckdb

# Input and output files
input_file = r"D:/CIR/prefect_ELT/roads_FULLNAME.parquet"
output_file = r"D:/CIR/prefect_ELT/roads_FULLNAME_dedup.parquet"

# Open a DuckDB connection (in-memory by default, but very efficient)
con = duckdb.connect()

# Register parquet file as a virtual table
con.execute(f"""
    CREATE VIEW parquet_data AS 
    SELECT * FROM read_parquet('{input_file}')
""")

# Deduplicate rows based on geometry column
# Replace 'geometry' with the actual geometry column name
con.execute(f"""
    COPY (
        SELECT DISTINCT ON (geometry) *
        FROM parquet_data
    ) TO '{output_file}' (FORMAT PARQUET)
""")

print(f"Deduplicated file written to: {output_file}")
