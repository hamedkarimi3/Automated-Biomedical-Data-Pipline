from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import pandas as pd
import json
import os

# Define default arguments for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define dataset output path (registered for Airflow UI tracking)
dataset_output = Dataset("/opt/airflow/data/processed/transformed_data.csv")

# Ensure processed directory exists
os.makedirs("/opt/airflow/data/processed", exist_ok=True)

# Define ETL Functions
def extract_data(**kwargs):
    """Extract patient records and genomic data from CSV and JSON."""
    print("Extracting data...")

    try:
        # Load Patient Records (CSV)
        patient_df = pd.read_csv("/opt/airflow/data/raw_new/patient_records.csv")

        # Load Genomic Data (JSON)
        with open("/opt/airflow/data/raw_new/genomics_data.json", "r") as file:
            genomic_data = json.load(file)
        genomic_df = pd.DataFrame(genomic_data)

        # Load Clinical Trials (CSV)
        clinical_trials_df = pd.read_csv("/opt/airflow/data/raw_new/clinical_trials.csv")

        print("Extraction complete.")

        # Save extracted data as CSV for further processing
        patient_path = "/opt/airflow/data/processed/patient_records.csv"
        genomic_path = "/opt/airflow/data/processed/genomics_data.csv"
        clinical_path = "/opt/airflow/data/processed/clinical_trials.csv"

        patient_df.to_csv(patient_path, index=False)
        genomic_df.to_csv(genomic_path, index=False)
        clinical_trials_df.to_csv(clinical_path, index=False)

        # Push file paths to XCom instead of raw data
        ti = kwargs["ti"]
        ti.xcom_push(key="patient_path", value=patient_path)
        ti.xcom_push(key="genomic_path", value=genomic_path)
        ti.xcom_push(key="clinical_path", value=clinical_path)

    except Exception as e:
        print(f"Extraction failed: {str(e)}")
        raise

def transform_data(**kwargs):
    """Transform and merge extracted data."""
    print("Transforming data...")

    try:
        ti = kwargs["ti"]

        # Retrieve file paths from XCom
        patient_path = ti.xcom_pull(task_ids="extract", key="patient_path")
        genomic_path = ti.xcom_pull(task_ids="extract", key="genomic_path")
        clinical_path = ti.xcom_pull(task_ids="extract", key="clinical_path")

        # Read from saved CSVs
        patient_df = pd.read_csv(patient_path)
        genomic_df = pd.read_csv(genomic_path)
        clinical_trials_df = pd.read_csv(clinical_path)

        # Merge Patient & Genomic Data
        merged_df = pd.merge(patient_df, genomic_df, on="patient_id", how="left")

        # Merge Clinical Trial Data
        final_df = pd.merge(merged_df, clinical_trials_df, on="patient_id", how="left")

        # Convert date format
        final_df["date_admitted"] = pd.to_datetime(final_df["date_admitted"])
        final_df["date"] = pd.to_datetime(final_df["date"])

        print("Transformation complete.")

        # Save transformed data as CSV
        final_path = "/opt/airflow/data/processed/transformed_data.csv"
        final_df.to_csv(final_path, index=False)

        # Push transformed data file path to XCom
        ti.xcom_push(key="final_path", value=final_path)

    except Exception as e:
        print(f"Transformation failed: {str(e)}")
        raise

def load_data(**kwargs):
    """Load transformed data into storage and register dataset update."""
    print("Loading data...")

    try:
        ti = kwargs["ti"]
        final_path = ti.xcom_pull(task_ids="transform", key="final_path")

        if final_path and os.path.exists(final_path):
            print(f"Data successfully loaded from: {final_path}")

            # Register dataset update in Airflow
            ti.xcom_push(key="dataset_output", value=dataset_output.uri)
        else:
            raise FileNotFoundError("Error: Transformed data file not found.")

    except Exception as e:
        print(f"Loading failed: {str(e)}")
        raise

# Define Airflow DAG
with DAG(
    "biomedical_etl_pipeline",
    default_args=default_args,
    description="A simple ETL pipeline for biomedical data",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
        provide_context=True
    )

    task_transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        provide_context=True
    )

    task_load = PythonOperator(
        task_id="load",
        python_callable=load_data,
        provide_context=True,
        outlets=[dataset_output]  # Register dataset in Airflow UI
    )

    # Define task dependencies
    task_extract >> task_transform >> task_load
