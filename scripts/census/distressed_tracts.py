import pandas as pd
import duckdb

def process_distressed_tracts(input_csv_path, output_path=None):
    """
    Loads and cleans the 2018 distressed/underserved tracts CSV file.
    Creates a properly formatted tract_fips column.
    
    Args:
        input_csv_path (str): Path to the CSV file.
        output_path (str): Optional path to save the cleaned file as Parquet.
    
    Returns:
        pd.DataFrame: Cleaned DataFrame with tract_fips and designation flags.
    """
    # Load the file with correct header row
    df = pd.read_csv(input_csv_path, skiprows=[0, 1], header=0)

    # Standardize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # Convert FIPS components to numeric with error handling
    df["state_code"] = pd.to_numeric(df["state_code"], errors="coerce")
    df["county_code"] = pd.to_numeric(df["county_code"], errors="coerce")
    df["tract_code"] = df["tract_code"].astype(str).str.replace(".", "", regex=False)
    df["tract_code"] = pd.to_numeric(df["tract_code"], errors="coerce")

    # Drop invalid rows
    df = df.dropna(subset=["state_code", "county_code", "tract_code"]).copy()

    # Format FIPS components
    df["state_code"] = df["state_code"].astype(int).astype(str).str.zfill(2)
    df["county_code"] = df["county_code"].astype(int).astype(str).str.zfill(3)
    df["tract_code"] = df["tract_code"].astype(int).astype(str).str.zfill(6)

    # Combine into tract_fips
    df["tract_fips"] = df["state_code"] + df["county_code"] + df["tract_code"]

    # Optional binary flags
    if 'distressed' not in df.columns.tolist(): 
        df["is_distressed"] = df[["poverty", "unemployment", "population_loss"]].fillna("").eq("X").any(axis=1).astype(int)
        df["is_underserved"] = df["remote_rural"].fillna("").eq("X").astype(int)
    else:
        df["is_distressed"] = df["distressed"].fillna("").str.upper().eq("X").astype(int)
        df["is_underserved"] = df["under_served"].fillna("").str.upper().eq("X").astype(int)

    # Save to Parquet if specified
    if output_path:
        df.to_parquet(output_path, index=False)
        print(f"✅ Saved cleaned data to {output_path}")

    return df

inital_cols = []
dfs = []
for year in range(2018, 2024): 
    file_path = f"/Users/charlesclark/Documents/WGU/Capstone/HMDA_Analysis/hmda_analytics_pipeline/scripts/census/data/{year}distressedorunderservedtracts.csv"
    df = process_distressed_tracts(file_path)
    if year == 2018: 
        initial_cols = df.columns.tolist()
    missing_cols = [x for x in df.columns.tolist() if x not in initial_cols]
    print(f"{year} - {missing_cols}")
    df['year'] = int(year)
    cols = ['year'] + [col for col in df.columns if col != year]
    df = df[['year', 'county_name', 'state_name', 'poverty', 'unemployment', 'population_loss', 'remote_rural', 'state_code', 'county_code', 'tract_code', 'tract_fips', 'is_distressed', 'is_underserved']]
    dfs.append(df)

all_years_df = pd.concat(dfs, ignore_index=True)
print(f"All years count: {all_years_df.shape}")

print(f"Inserting rows into duckdb")
con = duckdb.connect('/Volumes/T9/hmda_project/post_model_scores.duckdb')
con.execute("create table if not exists distressed_census_tracts_raw as select * from all_years_df limit 0")
con.execute("insert into distressed_census_tracts_raw select * from all_years_df")
print(f"Inserted {all_years_df.shape[0]} rows into table")
con.close()