from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json

# Define default arguments for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define ETL Functions
def extract_data(**kwargs):
    """Extract patient records and genomic data from CSV and JSON."""
    print("Extracting data...")

    # Load Patient Records (CSV)
    patient_df = pd.read_csv("/opt/airflow/data/raw_new/patient_records.csv")

    # Load Genomic Data (JSON)
    with open("/opt/airflow/data/raw_new/genomics_data.json", "r") as file:
        genomic_data = json.load(file)
    genomic_df = pd.DataFrame(genomic_data)

    # Load Clinical Trials (CSV) - **Fixed Path**
    clinical_trials_df = pd.read_csv("/opt/airflow/data/raw_new/clinical_trials.csv")

    print("Extraction complete.")

    # Save extracted data as CSV for XCom size limit issues
    patient_df.to_csv("/opt/airflow/data/processed/patient_records.csv", index=False)
    genomic_df.to_csv("/opt/airflow/data/processed/genomics_data.csv", index=False)
    clinical_trials_df.to_csv("/opt/airflow/data/processed/clinical_trials.csv", index=False)

    # Push file paths to XCom instead of raw data
    ti = kwargs["ti"]
    ti.xcom_push(key="patient_path", value="/opt/airflow/data/processed/patient_records.csv")
    ti.xcom_push(key="genomic_path", value="/opt/airflow/data/processed/genomics_data.csv")
    ti.xcom_push(key="clinical_path", value="/opt/airflow/data/processed/clinical_trials.csv")

def transform_data(**kwargs):
    """Transform and merge extracted data."""
    print("Transforming data...")

    ti = kwargs["ti"]

    # Retrieve file paths from XCom
    patient_path = ti.xcom_pull(task_ids="extract", key="patient_path")
    genomic_path = ti.xcom_pull(task_ids="extract", key="genomic_path")
    clinical_path = ti.xcom_pull(task_ids="extract", key="clinical_path")

    # Read from saved CSVs instead of XCom JSON
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

    # Push path to XCom
    ti.xcom_push(key="final_path", value=final_path)

def load_data(**kwargs):
    """Load transformed data into storage."""
    print("Loading data...")

    ti = kwargs["ti"]
    final_path = ti.xcom_pull(task_ids="transform", key="final_path")

    if final_path:
        print(f"Data successfully loaded from: {final_path}")
    else:
        print("Error: No data path found in XCom.")

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
        provide_context=True
    )

    # Define task dependencies
    task_extract >> task_transform >> task_load
