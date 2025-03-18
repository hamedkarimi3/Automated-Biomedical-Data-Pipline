# Automated Biomedical Data Pipeline

## Overview
This project implements an **end-to-end automated ETL pipeline** to process biomedical research data at **Karolinska Institute**. The pipeline ensures efficient ingestion, transformation, validation, and storage of large-scale omics and patient data.

## Features
✅ **ETL pipeline** using **Apache Airflow** for orchestration  
✅ **Scalable storage** and **data warehouse** built with **Snowflake**  
✅ **Real-time streaming** integration with **Apache Kafka**  
✅ **Automated data validation** using **Great Expectations**  
✅ **Data visualization** using **Power BI** and **Tableau**  

## Tech Stack
- **Data Ingestion:** Apache Kafka, REST APIs, Python  
- **Processing:** Apache Airflow, Apache Spark (PySpark), SQL  
- **Storage:** Snowflake, Azure Blob Storage  
- **Data Quality:** Great Expectations, dbt  
- **Visualization:** Power BI, Tableau  

## Project Structure
```
Automated-Biomedical-Data-Pipeline/
│── airflow/                     # Airflow configuration & logs
│── dags/                        # Airflow DAGs for orchestrating workflows
│── data/                        # Raw, processed, and streaming data
│   ├── raw/                     # Unprocessed data files
│   ├── processed/               # Cleaned and transformed data
│── notebooks/                   # Jupyter notebooks for data analysis
│── scripts/                     # ETL scripts and data utilities
│   ├── data_loader.py           # Load data into the pipeline
│   ├── data_cleaning.py         # Data preprocessing scripts
│   ├── kafka_producer.py        # Kafka producer script
│   ├── spark_transform.py       # Spark-based transformations
│── visualizations/              # Dashboard & reports
│── docker-compose.yml           # Docker setup for the project
│── Dockerfile                   # Docker image configuration
│── requirements.txt              # Required Python dependencies
│── README.md                    # Project documentation
```

## Installation
### **1. Set Up Virtual Environment**
```bash
python -m venv venv
```
Activate the virtual environment:
- **Windows (PowerShell)**: `venv\Scripts\activate`
- **Mac/Linux**: `source venv/bin/activate`

### **2. Install Dependencies**
```bash
pip install -r requirements.txt
```

### **3. Run Docker Containers**
```bash
docker-compose up -d
```

## Running the Pipeline
- **Start Airflow Scheduler & Webserver:**
  ```bash
  airflow scheduler & airflow webserver
  ```
- **Trigger a DAG manually:**
  ```bash
  airflow dags trigger <dag_id>
  ```
- **Check Logs:**
  ```bash
  airflow dags list
  airflow tasks list <dag_id>
  airflow tasks test <dag_id> <task_id> <execution_date>
  ```

## Contributions
Feel free to contribute by submitting pull requests, reporting issues, or suggesting improvements.

## License
This project is licensed under the MIT License.

## Contact
For questions or collaborations, contact **Karolinska Institute** research team.

