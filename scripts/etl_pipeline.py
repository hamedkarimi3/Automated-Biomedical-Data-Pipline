import pandas as pd
import json

# Step 1: Extract - Load CSV and JSON data
def extract_data():
    print("Extracting data...")

    # Load Patient Records (CSV)
    patient_df = pd.read_csv("data/patient_records.csv")

    # Load Genomic Data (JSON)
    with open("data/genomics_data.json", "r") as file:
        genomic_data = json.load(file)
    genomic_df = pd.DataFrame(genomic_data)

    # Load Clinical Trials (CSV)
    clinical_trials_df = pd.read_csv("data/clinical_trials.csv")

    print("Extraction complete.")
    return patient_df, genomic_df, clinical_trials_df

# Step 2: Transform - Clean and Merge Data
def transform_data(patient_df, genomic_df, clinical_trials_df):
    print("Transforming data...")

    # Merge Patient & Genomic Data
    merged_df = pd.merge(patient_df, genomic_df, on="patient_id", how="left")

    # Merge Clinical Trial Data
    final_df = pd.merge(merged_df, clinical_trials_df, on="patient_id", how="left")

    # Convert date format
    final_df["date_admitted"] = pd.to_datetime(final_df["date_admitted"])
    final_df["date"] = pd.to_datetime(final_df["date"])

    print("Transformation complete.")
    return final_df

# Step 3: Load - Save the processed data
def load_data(final_df):
    print("Loading data into output file...")

    # Save transformed data as a new CSV file (simulating loading into Snowflake)
    final_df.to_csv("data/transformed_data.csv", index=False)

    print("Data successfully loaded.")

# Run ETL Process
if __name__ == "__main__":
    patients, genomics, trials = extract_data()
    transformed_data = transform_data(patients, genomics, trials)
    load_data(transformed_data)

    print("ETL pipeline executed successfully!")

