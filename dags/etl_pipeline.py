from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
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
def extract_data():
    """Extract patient records and genomic data from CSV and JSON."""
    print("Extracting data...")

    # Load Patient Records (CSV)
    patient_df = pd.read_csv("/opt/airflow/data/patient_records.csv")

    # Load Genomic Data (JSON)
    with open("/opt/airflow/data/genomics_data.json", "r") as file:
        genomic_data = json.load(file)
    genomic_df = pd.DataFrame(genomic_data)

    # Load Clinical Trials (CSV)
    clinical_trials_df = pd.read_csv("/opt/airflow/data/clinical_trials.csv")

    print("Extraction complete.")
    return patient_df.to_json(), genomic_df.to_json(), clinical_trials_df.to_json()

def transform_data(**kwargs):
    """Transform and merge extracted data."""
    print("Transforming data...")

    # Retrieve extracted data
    ti = kwargs["ti"]
    patient_json = ti.xcom_pull(task_ids="extract")
    genomic_json = ti.xcom_pull(task_ids="extract")
    clinical_json = ti.xcom_pull(task_ids="extract")

    # Convert JSON strings back to DataFrame
    patient_df = pd.read_json(patient_json)
    genomic_df = pd.read_json(genomic_json)
    clinical_trials_df = pd.read_json(clinical_json)

    # Merge Patient & Genomic Data
    merged_df = pd.merge(patient_df, genomic_df, on="patient_id", how="left")

    # Merge Clinical Trial Data
    final_df = pd.merge(merged_df, clinical_trials_df, on="patient_id", how="left")

    # Convert date format
    final_df["date_admitted"] = pd.to_datetime(final_df["date_admitted"])
    final_df["date"] = pd.to_datetime(final_df["date"])

    print("Transformation complete.")
    return final_df.to_json()

def load_data(**kwargs):
    """Load transformed data into storage."""
    print("Loading data...")

    # Retrieve transformed data
    ti = kwargs["ti"]
    final_json = ti.xcom_pull(task_ids="transform")
    final_df = pd.read_json(final_json)

    # Save transformed data as CSV
    final_df.to_csv("/opt/airflow/data/transformed_data.csv", index=False)

    print("Data successfully loaded.")

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
        python_callable=extract_data
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

