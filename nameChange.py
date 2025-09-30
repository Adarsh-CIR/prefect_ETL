import pandas as pd

# Input and output file paths
input_file = "D:\CIR\prefect_ELT\Secondary_Roads_Interstates_and_US_Highways.parquet"
output_file = "D:\CIR\prefect_ELT\Secondary_Roads_Interstates_and_US_Highways_Full.parquet"

# Read the Parquet file
df = pd.read_parquet(input_file)

# Rename the column
df = df.rename(columns={"NAME": "fullname"})

# Save back to a Parquet file
df.to_parquet(output_file, index=False)

print(f"✅ Column renamed and saved to {output_file}")
