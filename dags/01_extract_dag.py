from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "datamart_data_eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

def get_staging_dir(data_dir: str) -> str:
    staging_dir = os.path.join(data_dir, ".staging")
    os.makedirs(staging_dir, exist_ok=True)
    return staging_dir

def extract_daily_sales(**context):
    """Extrae transacciones diarias desde data/raw/ o data/."""
    data_dir = Variable.get("datamart_data_dir", default_var="/opt/airflow/data")
    staging_dir = get_staging_dir(data_dir)
    
    candidate_paths = [
        os.path.join(data_dir, "raw", "data.csv"),
        os.path.join(data_dir, "data.csv"),
    ]
    file_path = next((p for p in candidate_paths if os.path.exists(p)), None)
    if not file_path:
        raise FileNotFoundError(f"Archivo diario no encontrado en: {candidate_paths}")

    df = pd.read_csv(file_path, encoding="ISO-8859-1", dtype=str)
    df.columns = [c.strip() for c in df.columns]
    df["source_file"] = "daily_csv"
    
    out_path = os.path.join(staging_dir, "raw_daily.parquet")
    df.to_parquet(out_path, index=False)
    print(f"Extracción diaria completada: {len(df)} registros en {out_path}")
    return out_path

def extract_historical_sales(**context):
    """Extrae historial extendido y homologa su esquema."""
    data_dir = Variable.get("datamart_data_dir", default_var="/opt/airflow/data")
    staging_dir = get_staging_dir(data_dir)
    
    candidate_paths = [
        os.path.join(data_dir, "raw", "Sales Transaction v.4a.csv"),
        os.path.join(data_dir, "raw", "online_retail_II.csv"),
        os.path.join(data_dir, "Sales Transaction v.4a.csv"),
        os.path.join(data_dir, "online_retail_II.csv"),
    ]
    file_path = next((p for p in candidate_paths if os.path.exists(p)), None)
    if not file_path:
        raise FileNotFoundError(f"Archivo histórico no encontrado en: {candidate_paths}")

    df = pd.read_csv(file_path, encoding="ISO-8859-1", dtype=str)
    df.columns = [c.strip() for c in df.columns]
    
    rename_map = {
        "Invoice": "InvoiceNo",
        "TransactionNo": "InvoiceNo",
        "Price": "UnitPrice",
        "ProductNo": "StockCode",
        "ProductName": "Description",
        "Date": "InvoiceDate",
        "Customer ID": "CustomerID",
        "CustomerNo": "CustomerID",
    }
    df = df.rename(columns=rename_map)
    df["source_file"] = "historical_csv"
    
    out_path = os.path.join(staging_dir, "raw_historical.parquet")
    df.to_parquet(out_path, index=False)
    print(f"Extracción histórica completada: {len(df)} registros en {out_path}")
    return out_path

with DAG(
    dag_id="datamart_01_extract",
    default_args=default_args,
    description="Fase 1: Extracción paralela de datasets crudos",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datamart", "etl", "extract"],
) as dag:

    t_extract_daily = PythonOperator(
        task_id="extract_daily_csv",
        python_callable=extract_daily_sales,
    )

    t_extract_hist = PythonOperator(
        task_id="extract_historical_csv",
        python_callable=extract_historical_sales,
    )

    t_trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="datamart_02_transform",
        wait_for_completion=False,
    )

    [t_extract_daily, t_extract_hist] >> t_trigger_transform
