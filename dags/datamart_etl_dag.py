"""
DAG de Extracción, Transformación, Validación y Carga Idempotente
Caso: DataMart S.A.S. - Data Warehouse Analítico
"""

from datetime import datetime, timedelta
import hashlib
import os
import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

default_args = {
    "owner": "datamart_data_eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

STAGING_DIR = "/tmp/datamart_staging"
os.makedirs(STAGING_DIR, exist_ok=True)

def extract_daily_sales(**context):
    """Extrae data.csv (Fuente 1: Transacciones Diarias) y estandariza nombres de columnas."""
    data_dir = Variable.get("datamart_data_dir", default_var="/opt/airflow/data")
    
    candidate_paths = [
        os.path.join(data_dir, "data.csv"),
        os.path.join(data_dir, "Data", "data.csv"),
    ]
    file_path = next((p for p in candidate_paths if os.path.exists(p)), None)
    if not file_path:
        raise FileNotFoundError(f"Archivo diario no encontrado en candidatos: {candidate_paths}")

    df = pd.read_csv(file_path, encoding="ISO-8859-1", dtype=str)
    df.columns = [c.strip() for c in df.columns]
    df["source_file"] = "daily_csv"
    
    out_path = os.path.join(STAGING_DIR, "raw_daily.parquet")
    df.to_parquet(out_path, index=False)
    return out_path

def extract_historical_sales(**context):
    """Extrae el histórico (Fuente 2) y homologa su esquema al de Fuente 1."""
    data_dir = Variable.get("datamart_data_dir", default_var="/opt/airflow/data")
    
    # Busca online_retail_II.csv o Sales Transaction v.4a.csv en data_dir y subcarpetas
    candidate_paths = [
        os.path.join(data_dir, "online_retail_II.csv"),
        os.path.join(data_dir, "Sales Transaction v.4a.csv"),
        os.path.join(data_dir, "Data-II", "Sales Transaction v.4a.csv"),
    ]
    file_path = next((p for p in candidate_paths if os.path.exists(p)), None)
    if not file_path:
        raise FileNotFoundError(f"Archivo histórico no encontrado en candidatos: {candidate_paths}")

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
    
    out_path = os.path.join(STAGING_DIR, "raw_historical.parquet")
    df.to_parquet(out_path, index=False)
    return out_path

def transform_and_validate(**context):
    """Aplica reglas de negocio, resuelve casos ambiguos, registra rechazos y enriquece categorías."""
    ti = context["ti"]
    daily_path = ti.xcom_pull(task_ids="extract_daily_csv")
    hist_path = ti.xcom_pull(task_ids="extract_historical_csv")
    api_url = Variable.get("datamart_api_url", default_var="http://products-api:8000")

    df_daily = pd.read_parquet(daily_path)
    df_hist = pd.read_parquet(hist_path)

    # 1. Unificación dando prioridad a la Fuente 1 (volcado operacional vigente)
    df_all = pd.concat([df_daily, df_hist], ignore_index=True)

    # Normalización de cadenas y eliminación de espacios
    df_all["StockCode_clean"] = df_all["StockCode"].fillna("").astype(str).str.strip().str.upper()
    df_all["InvoiceNo_clean"] = df_all["InvoiceNo"].fillna("").astype(str).str.strip().str.upper()
    df_all["Description_clean"] = (
        df_all["Description"].fillna("UNKNOWN PRODUCT").astype(str).str.strip().str.upper()
        .str.replace(r"\s+", " ", regex=True)
    )
    df_all["Country_clean"] = df_all["Country"].fillna("UNKNOWN").astype(str).str.strip().str.upper()

    # Conversión numérica y fechas estandarizadas a UTC
    df_all["Quantity_num"] = pd.to_numeric(df_all["Quantity"], errors="coerce")
    df_all["UnitPrice_num"] = pd.to_numeric(df_all["UnitPrice"], errors="coerce")
    df_all["InvoiceDate_utc"] = pd.to_datetime(df_all["InvoiceDate"], errors="coerce", utc=True)

    # --- 2. CONTROL DE CALIDAD Y REGISTRO DE RECHAZOS ---
    invalid_mask = (
        df_all["StockCode_clean"].eq("")
        | df_all["InvoiceNo_clean"].eq("")
        | df_all["InvoiceDate_utc"].isna()
        | df_all["Quantity_num"].isna()
        | df_all["UnitPrice_num"].isna()
        | (df_all["UnitPrice_num"] <= 0)
    )

    df_rejected = df_all[invalid_mask].copy()
    def get_reason(row):
        if pd.isna(row["UnitPrice_num"]) or row["UnitPrice_num"] <= 0:
            return "INVALID_UNIT_PRICE_LTE_ZERO"
        if pd.isna(row["InvoiceDate_utc"]):
            return "INVALID_DATE_FORMAT"
        if pd.isna(row["Quantity_num"]):
            return "INVALID_QUANTITY"
        return "MISSING_MANDATORY_IDENTIFIERS"

    if not df_rejected.empty:
        df_rejected["rejection_reason"] = df_rejected.apply(get_reason, axis=1)
        df_rejected["record_hash"] = df_rejected.apply(
            lambda r: hashlib.sha256(
                f"{r['InvoiceNo']}|{r['StockCode']}|{r['Quantity']}|{r['UnitPrice']}|{r['InvoiceDate']}|{r['source_file']}".encode()
            ).hexdigest(),
            axis=1,
        )

    df_rejected.to_parquet(os.path.join(STAGING_DIR, "rejected_records.parquet"), index=False)

    # --- 3. PROCESAMIENTO DE REGISTROS VÁLIDOS ---
    df_valid = df_all[~invalid_mask].copy()

    # Caso Ambiguo 1: Sin CustomerID -> 'GUEST' con is_identified = False
    df_valid["CustomerID_clean"] = (
        df_valid["CustomerID"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )
    df_valid["is_identified"] = df_valid["CustomerID_clean"] != ""
    df_valid.loc[~df_valid["is_identified"], "CustomerID_clean"] = "GUEST"

    # Caso Ambiguo 3: Deduplicación entre fuentes por clave natural compuesta
    df_valid["dedup_key"] = (
        df_valid["InvoiceNo_clean"] + "|" +
        df_valid["StockCode_clean"] + "|" +
        df_valid["Quantity_num"].astype(int).astype(str) + "|" +
        df_valid["UnitPrice_num"].round(2).astype(str) + "|" +
        df_valid["InvoiceDate_utc"].dt.strftime("%Y-%m-%d %H:%M")
    )
    df_valid = df_valid.drop_duplicates(subset=["dedup_key"], keep="first").copy()

    # Regla de Negocio: Clasificación de Ventas (>0) vs Devoluciones (<=0)
    df_valid["Quantity_int"] = df_valid["Quantity_num"].astype(int)
    df_valid["transaction_type"] = df_valid["Quantity_int"].apply(lambda q: "SALE" if q > 0 else "RETURN")
    df_valid["gross_amount"] = (df_valid["Quantity_int"].abs() * df_valid["UnitPrice_num"]).round(2)
    df_valid["signed_amount"] = (df_valid["Quantity_int"] * df_valid["UnitPrice_num"]).round(2)
    df_valid["date_id"] = df_valid["InvoiceDate_utc"].dt.date

    # Hash determinista de transacción para inserción idempotente
    df_valid["transaction_hash"] = df_valid["dedup_key"].apply(
        lambda k: hashlib.sha256(k.encode()).hexdigest()
    )

    # Caso Ambiguo 2: Moda estadística para nombre canónico y conteo de variantes
    raw_variants = (
        df_valid.groupby("StockCode_clean")["Description"]
        .nunique(dropna=True)
        .reset_index(name="raw_description_variants_count")
    )
    canonical_desc = (
        df_valid.groupby("StockCode_clean")["Description_clean"]
        .agg(lambda x: x.value_counts().index[0] if len(x.value_counts()) > 0 else "UNKNOWN PRODUCT")
        .reset_index(name="canonical_description")
    )
    df_products = pd.merge(canonical_desc, raw_variants, on="StockCode_clean", how="left")

    # Enriquecimiento con la API de Catálogo (o fallback local determinista)
    categories, countries, actives = [], [], []
    session = requests.Session()
    for _, row in df_products.iterrows():
        code = row["StockCode_clean"]
        desc = row["canonical_description"]
        try:
            resp = session.get(f"{api_url}/products/{code}", params={"description": desc}, timeout=1.5)
            if resp.status_code == 200:
                data = resp.json()
                categories.append(data["category"])
                countries.append(data["supplier_country"])
                actives.append(data["is_active"])
                continue
        except Exception:
            pass
        # Fallback local determinista si la API no está disponible
        categories.append("Hogar")
        countries.append("Colombia")
        actives.append(True)

    df_products["category"] = categories
    df_products["supplier_country"] = countries
    df_products["is_active"] = actives

    df_products.to_parquet(os.path.join(STAGING_DIR, "dim_products.parquet"), index=False)
    df_valid.to_parquet(os.path.join(STAGING_DIR, "valid_transactions.parquet"), index=False)

def load_to_dwh(**context):
    """Carga idempotente (Upsert) en PostgreSQL DWH usando ON CONFLICT."""
    pg_hook = PostgresHook(postgres_conn_id="datamart_dwh_conn")
    conn = pg_hook.get_conn()
    batch_size = int(Variable.get("datamart_batch_size", default_var="10000"))

    df_products = pd.read_parquet(os.path.join(STAGING_DIR, "dim_products.parquet"))
    df_valid = pd.read_parquet(os.path.join(STAGING_DIR, "valid_transactions.parquet"))
    df_rejected = pd.read_parquet(os.path.join(STAGING_DIR, "rejected_records.parquet"))

    with conn.cursor() as cur:
        # 1. Cargar dim_date
        unique_dates = pd.to_datetime(df_valid["date_id"].unique())
        date_rows = [
            (d.date(), d.year, d.month, d.strftime("%Y-%m"), d.dayofweek, d.dayofweek >= 5)
            for d in unique_dates
        ]
        execute_values(
            cur,
            """
            INSERT INTO dim_date (date_id, year, month, year_month, day_of_week, is_weekend)
            VALUES %s
            ON CONFLICT (date_id) DO NOTHING;
            """,
            date_rows,
            page_size=batch_size,
        )

        # 2. Cargar dim_product
        prod_rows = [
            (
                r["StockCode_clean"],
                r["canonical_description"][:255],
                r["category"],
                r["supplier_country"],
                bool(r["is_active"]),
                int(r["raw_description_variants_count"]),
            )
            for _, r in df_products.iterrows()
        ]
        execute_values(
            cur,
            """
            INSERT INTO dim_product (
                stock_code, canonical_description, category, supplier_country, is_active, raw_description_variants_count
            ) VALUES %s
            ON CONFLICT (stock_code) DO UPDATE SET
                canonical_description = EXCLUDED.canonical_description,
                category = EXCLUDED.category,
                raw_description_variants_count = EXCLUDED.raw_description_variants_count,
                updated_at = CURRENT_TIMESTAMP;
            """,
            prod_rows,
            page_size=batch_size,
        )

        # 3. Cargar dim_customer
        df_cust = df_valid.groupby("CustomerID_clean").agg(
            is_identified=("is_identified", "first"),
            first_seen_country=("Country_clean", "first"),
        ).reset_index()
        cust_rows = [
            (r["CustomerID_clean"], bool(r["is_identified"]), r["first_seen_country"])
            for _, r in df_cust.iterrows()
        ]
        execute_values(
            cur,
            """
            INSERT INTO dim_customer (customer_id, is_identified, first_seen_country)
            VALUES %s
            ON CONFLICT (customer_id) DO UPDATE SET
                first_seen_country = EXCLUDED.first_seen_country;
            """,
            cust_rows,
            page_size=batch_size,
        )

        # 4. Cargar fact_transactions (Idempotente vía transaction_hash)
        fact_rows = [
            (
                r["transaction_hash"],
                r["InvoiceNo_clean"],
                r["StockCode_clean"],
                r["CustomerID_clean"],
                r["date_id"],
                r["InvoiceDate_utc"].to_pydatetime(),
                r["Country_clean"],
                r["transaction_type"],
                int(r["Quantity_int"]),
                float(r["UnitPrice_num"]),
                float(r["gross_amount"]),
                float(r["signed_amount"]),
                r["source_file"],
            )
            for _, r in df_valid.iterrows()
        ]
        execute_values(
            cur,
            """
            INSERT INTO fact_transactions (
                transaction_hash, invoice_no, stock_code, customer_id, date_id,
                invoice_timestamp_utc, country, transaction_type, quantity,
                unit_price, gross_amount, signed_amount, source_file
            ) VALUES %s
            ON CONFLICT (transaction_hash) DO UPDATE SET
                gross_amount = EXCLUDED.gross_amount,
                signed_amount = EXCLUDED.signed_amount,
                loaded_at = CURRENT_TIMESTAMP;
            """,
            fact_rows,
            page_size=batch_size,
        )

        # 5. Calcular tabla agregada fact_daily_product_summary
        cur.execute("""
            INSERT INTO fact_daily_product_summary (
                summary_id, date_id, stock_code, sales_quantity, returns_quantity,
                gross_revenue, returns_revenue, net_revenue, updated_at
            )
            SELECT
                date_id::text || '_' || stock_code AS summary_id,
                date_id,
                stock_code,
                SUM(CASE WHEN transaction_type = 'SALE' THEN quantity ELSE 0 END) AS sales_quantity,
                SUM(CASE WHEN transaction_type = 'RETURN' THEN ABS(quantity) ELSE 0 END) AS returns_quantity,
                SUM(CASE WHEN transaction_type = 'SALE' THEN gross_amount ELSE 0 END) AS gross_revenue,
                SUM(CASE WHEN transaction_type = 'RETURN' THEN gross_amount ELSE 0 END) AS returns_revenue,
                SUM(signed_amount) AS net_revenue,
                CURRENT_TIMESTAMP
            FROM fact_transactions
            GROUP BY date_id, stock_code
            ON CONFLICT (summary_id) DO UPDATE SET
                sales_quantity = EXCLUDED.sales_quantity,
                returns_quantity = EXCLUDED.returns_quantity,
                gross_revenue = EXCLUDED.gross_revenue,
                returns_revenue = EXCLUDED.returns_revenue,
                net_revenue = EXCLUDED.net_revenue,
                updated_at = CURRENT_TIMESTAMP;
        """)

        # 6. Cargar registros rechazados en audit_rejected_records
        if not df_rejected.empty:
            df_rejected = df_rejected.drop_duplicates(subset=["record_hash"])
            rej_rows = [
                (
                    r["record_hash"],
                    str(r.get("InvoiceNo", ""))[:50],
                    str(r.get("StockCode", ""))[:50],
                    str(r.get("Description", "")),
                    str(r.get("Quantity", ""))[:50],
                    str(r.get("UnitPrice", ""))[:50],
                    str(r.get("InvoiceDate", ""))[:50],
                    str(r.get("CustomerID", ""))[:50],
                    str(r.get("Country", ""))[:100],
                    r["source_file"],
                    r["rejection_reason"],
                )
                for _, r in df_rejected.iterrows()
            ]
            execute_values(
                cur,
                """
                INSERT INTO audit_rejected_records (
                    record_hash, raw_invoice_no, raw_stock_code, raw_description,
                    raw_quantity, raw_unit_price, raw_invoice_date, raw_customer_id,
                    raw_country, source_file, rejection_reason
                ) VALUES %s
                ON CONFLICT (record_hash) DO NOTHING;
                """,
                rej_rows,
                page_size=batch_size,
            )

    conn.commit()
    conn.close()

with DAG(
    dag_id="datamart_etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL Idempotente para DataMart S.A.S.",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datamart", "etl", "dwh"],
) as dag:

    t_extract_daily = PythonOperator(
        task_id="extract_daily_csv",
        python_callable=extract_daily_sales,
    )

    t_extract_hist = PythonOperator(
        task_id="extract_historical_csv",
        python_callable=extract_historical_sales,
    )

    t_transform = PythonOperator(
        task_id="transform_validate_and_enrich",
        python_callable=transform_and_validate,
    )

    t_load = PythonOperator(
        task_id="load_to_postgres_dwh",
        python_callable=load_to_dwh,
    )

    [t_extract_daily, t_extract_hist] >> t_transform >> t_load
