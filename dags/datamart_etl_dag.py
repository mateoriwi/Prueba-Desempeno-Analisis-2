"""
DAG de Extracción, Transformación, Validación y Carga Idempotente (Monolítico)
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

CATEGORIES = ["Electrónica", "Hogar", "Ropa", "Deportes", "Papelería"]
COUNTRIES = ["Colombia", "México", "Perú", "China", "Estados Unidos"]

def fallback_classify(code: str, description: str) -> dict:
    """Clasificación heurística y determinista equivalente a la API de catálogo."""
    clean_code = str(code).strip().upper()
    desc_upper = (str(description) if description else f"PRODUCT {clean_code}").strip().upper()
    
    if any(k in desc_upper for k in ["LIGHT", "LAMP", "CLOCK", "ALARM", "RADIO", "USB", "TORCH", "FAN"]):
        category = "Electrónica"
    elif any(k in desc_upper for k in ["BAG", "SHIRT", "SOCK", "HAT", "APRON", "PURSE", "SCARF", "UMBRELLA"]):
        category = "Ropa"
    elif any(k in desc_upper for k in ["PEN", "PENCIL", "NOTEBOOK", "CARD", "PAPER", "STICKER", "TAPE", "BOOK"]):
        category = "Papelería"
    elif any(k in desc_upper for k in ["BALL", "GAME", "BIKE", "JUMPING", "SPORT", "FLASK", "BOTTLE", "CAMPING"]):
        category = "Deportes"
    else:
        category = "Hogar"
        
    h = int(hashlib.md5(clean_code.encode()).hexdigest(), 16)
    return {
        "category": category,
        "supplier_country": COUNTRIES[h % len(COUNTRIES)],
        "is_active": (h % 10) != 0,
    }

def extract_daily_sales(**context):
    """Extrae data.csv (Fuente 1: Transacciones Diarias) y estandariza nombres de columnas."""
    data_dir = Variable.get("datamart_data_dir", default_var="/opt/airflow/data")
    staging_dir = os.path.join(data_dir, ".staging")
    os.makedirs(staging_dir, exist_ok=True)
    
    candidate_paths = [
        os.path.join(data_dir, "raw", "data.csv"),
        os.path.join(data_dir, "data.csv"),
    ]
    file_path = next((p for p in candidate_paths if os.path.exists(p)), None)
    if not file_path:
        raise FileNotFoundError(f"Archivo diario no encontrado en candidatos: {candidate_paths}")

    df = pd.read_csv(file_path, encoding="ISO-8859-1", dtype=str)
    df.columns = [c.strip() for c in df.columns]
    df["source_file"] = "daily_csv"
    
    out_path = os.path.join(staging_dir, "raw_daily.parquet")
    df.to_parquet(out_path, index=False)
    print(f"Extracción diaria completada: {len(df)} filas.")
    return out_path

def extract_historical_sales(**context):
    """Extrae el histórico (Fuente 2) y homologa su esquema al de Fuente 1."""
    data_dir = Variable.get("datamart_data_dir", default_var="/opt/airflow/data")
    staging_dir = os.path.join(data_dir, ".staging")
    os.makedirs(staging_dir, exist_ok=True)
    
    candidate_paths = [
        os.path.join(data_dir, "raw", "Sales Transaction v.4a.csv"),
        os.path.join(data_dir, "raw", "online_retail_II.csv"),
        os.path.join(data_dir, "Sales Transaction v.4a.csv"),
        os.path.join(data_dir, "online_retail_II.csv"),
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
    
    out_path = os.path.join(staging_dir, "raw_historical.parquet")
    df.to_parquet(out_path, index=False)
    print(f"Extracción histórica completada: {len(df)} filas.")
    return out_path

def transform_and_validate(**context):
    """Aplica reglas de negocio, resuelve casos ambiguos, exporta a data/clean/ y prepara para carga."""
    data_dir = Variable.get("datamart_data_dir", default_var="/opt/airflow/data")
    staging_dir = os.path.join(data_dir, ".staging")
    os.makedirs(staging_dir, exist_ok=True)
    clean_dir = os.path.join(data_dir, "clean")
    os.makedirs(clean_dir, exist_ok=True)

    daily_path = os.path.join(staging_dir, "raw_daily.parquet")
    hist_path = os.path.join(staging_dir, "raw_historical.parquet")

    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection("products_api_conn")
        schema = conn.schema or "http"
        host = conn.host or "products-api"
        port = f":{conn.port}" if conn.port else ":8000"
        api_url = f"{schema}://{host}{port}"
    except Exception:
        api_url = Variable.get("datamart_api_url", default_var="http://products-api:8000")

    df_daily = pd.read_parquet(daily_path)
    df_hist = pd.read_parquet(hist_path)

    # 1. Unificación dando prioridad a la Fuente 1 (volcado operacional vigente)
    df_all = pd.concat([df_daily, df_hist], ignore_index=True)

    # Normalización de strings
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
    try:
        df_all["InvoiceDate_utc"] = pd.to_datetime(df_all["InvoiceDate"], errors="coerce", utc=True, format="mixed")
    except Exception:
        df_all["InvoiceDate_utc"] = pd.to_datetime(df_all["InvoiceDate"], errors="coerce", utc=True)

    # --- 2. CONTROL DE CALIDAD Y RECHAZOS ---
    invalid_mask = (
        df_all["StockCode_clean"].eq("")
        | df_all["InvoiceNo_clean"].eq("")
        | df_all["InvoiceDate_utc"].isna()
        | df_all["Quantity_num"].isna()
        | (df_all["Quantity_num"] == 0)
        | df_all["UnitPrice_num"].isna()
        | (df_all["UnitPrice_num"] <= 0)
    )

    df_rejected = df_all[invalid_mask].copy()
    def get_reason(row):
        if pd.isna(row["UnitPrice_num"]) or row["UnitPrice_num"] <= 0:
            return "INVALID_UNIT_PRICE_LTE_ZERO"
        if pd.isna(row["InvoiceDate_utc"]):
            return "INVALID_DATE_FORMAT"
        if pd.isna(row["Quantity_num"]) or row["Quantity_num"] == 0:
            return "INVALID_OR_ZERO_QUANTITY"
        return "MISSING_MANDATORY_IDENTIFIERS"

    if not df_rejected.empty:
        df_rejected["rejection_reason"] = df_rejected.apply(get_reason, axis=1)
        df_rejected["record_hash"] = df_rejected.apply(
            lambda r: hashlib.sha256(
                f"{r.get('InvoiceNo','')}|{r.get('StockCode','')}|{r.get('Quantity','')}|{r.get('UnitPrice','')}|{r.get('InvoiceDate','')}|{r.get('source_file','')}".encode()
            ).hexdigest(),
            axis=1,
        )

    # Guarda staging y exporta a carpeta local data/clean/
    df_rejected.to_parquet(os.path.join(staging_dir, "rejected_records.parquet"), index=False)
    df_rejected.to_csv(os.path.join(clean_dir, "registros_rechazados.csv"), index=False)

    # --- 3. PROCESAMIENTO DE REGISTROS VÁLIDOS ---
    df_valid = df_all[~invalid_mask].copy()

    # Caso Ambiguo 1: Sin CustomerID -> 'GUEST'
    df_valid["CustomerID_clean"] = (
        df_valid["CustomerID"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )
    df_valid["is_identified"] = df_valid["CustomerID_clean"] != ""
    df_valid.loc[~df_valid["is_identified"], "CustomerID_clean"] = "GUEST"

    # Caso Ambiguo 3: Deduplicación entre fuentes
    df_valid["dedup_key"] = (
        df_valid["InvoiceNo_clean"] + "|" +
        df_valid["StockCode_clean"] + "|" +
        df_valid["Quantity_num"].astype(int).astype(str) + "|" +
        df_valid["UnitPrice_num"].round(2).astype(str) + "|" +
        df_valid["InvoiceDate_utc"].dt.strftime("%Y-%m-%d %H:%M")
    )
    df_valid = df_valid.drop_duplicates(subset=["dedup_key"], keep="first").copy()

    # Separación Ventas vs Devoluciones
    df_valid["Quantity_int"] = df_valid["Quantity_num"].astype(int)
    df_valid["transaction_type"] = df_valid["Quantity_int"].apply(lambda q: "SALE" if q > 0 else "RETURN")
    df_valid["gross_amount"] = (df_valid["Quantity_int"].abs() * df_valid["UnitPrice_num"]).round(2)
    df_valid["signed_amount"] = (df_valid["Quantity_int"] * df_valid["UnitPrice_num"]).round(2)
    df_valid["date_id"] = df_valid["InvoiceDate_utc"].dt.date
    df_valid["transaction_hash"] = df_valid["dedup_key"].apply(
        lambda k: hashlib.sha256(k.encode()).hexdigest()
    )

    # Caso Ambiguo 2: Moda estadística para nombre canónico
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

    # Enriquecimiento con API de catálogo (con prueba previa y fallback determinista)
    session = requests.Session()
    api_available = False
    try:
        health_resp = session.get(f"{api_url}/health", timeout=2.0)
        if health_resp.status_code == 200:
            api_available = True
    except Exception:
        api_available = False

    categories, countries, actives = [], [], []
    for row in df_products.itertuples(index=False):
        code = row.StockCode_clean
        desc = row.canonical_description
        if api_available:
            try:
                resp = session.get(f"{api_url}/products/{code}", params={"description": desc}, timeout=0.8)
                if resp.status_code == 200:
                    data = resp.json()
                    categories.append(data.get("category", "Hogar"))
                    countries.append(data.get("supplier_country", "Colombia"))
                    actives.append(data.get("is_active", True))
                    continue
            except Exception:
                pass
        fb = fallback_classify(code, desc)
        categories.append(fb["category"])
        countries.append(fb["supplier_country"])
        actives.append(fb["is_active"])

    df_products["category"] = categories
    df_products["supplier_country"] = countries
    df_products["is_active"] = actives

    # Guarda staging interno Parquet para la tarea de carga
    df_products.to_parquet(os.path.join(staging_dir, "dim_products.parquet"), index=False)
    df_valid.to_parquet(os.path.join(staging_dir, "valid_transactions.parquet"), index=False)

    # Exportación a disco local data/clean/
    df_valid.to_csv(os.path.join(clean_dir, "transacciones_limpias.csv"), index=False)
    df_products.to_csv(os.path.join(clean_dir, "productos_catalogo_limpio.csv"), index=False)
    print(f"Transformación y exportación local completada: {len(df_valid)} transacciones limpias.")

def load_to_dwh(**context):
    """Carga idempotente (Upsert) en PostgreSQL DWH usando ON CONFLICT."""
    pg_hook = PostgresHook(postgres_conn_id="datamart_dwh_conn")
    conn = pg_hook.get_conn()
    batch_size = int(Variable.get("datamart_batch_size", default_var="10000"))
    data_dir = Variable.get("datamart_data_dir", default_var="/opt/airflow/data")
    staging_dir = os.path.join(data_dir, ".staging")

    df_products = pd.read_parquet(os.path.join(staging_dir, "dim_products.parquet"))
    df_valid = pd.read_parquet(os.path.join(staging_dir, "valid_transactions.parquet"))
    df_rejected = pd.read_parquet(os.path.join(staging_dir, "rejected_records.parquet"))

    with conn.cursor() as cur:
        # 1. Cargar dim_date
        clean_dates = pd.to_datetime(df_valid["date_id"].dropna().unique(), errors="coerce")
        date_rows = [
            (d.date(), int(d.year), int(d.month), str(d.strftime("%Y-%m")), int(d.dayofweek), bool(d.dayofweek >= 5))
            for d in clean_dates if pd.notna(d)
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
                str(r.StockCode_clean)[:50],
                str(r.canonical_description)[:255],
                str(r.category)[:50],
                str(r.supplier_country)[:50],
                bool(r.is_active),
                int(r.raw_description_variants_count),
            )
            for r in df_products.itertuples(index=False)
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
            (str(r.CustomerID_clean)[:50], bool(r.is_identified), str(r.first_seen_country)[:100])
            for r in df_cust.itertuples(index=False)
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

        # 4. Cargar fact_transactions por lotes (Optimizado con itertuples posicional)
        total_valid = len(df_valid)
        insert_fact_sql = """
            INSERT INTO fact_transactions (
                transaction_hash, invoice_no, stock_code, customer_id, date_id,
                invoice_timestamp_utc, country, transaction_type, quantity,
                unit_price, gross_amount, signed_amount, source_file
            ) VALUES %s
            ON CONFLICT (transaction_hash) DO UPDATE SET
                gross_amount = EXCLUDED.gross_amount,
                signed_amount = EXCLUDED.signed_amount,
                loaded_at = CURRENT_TIMESTAMP;
        """

        fact_cols = [
            "transaction_hash", "InvoiceNo_clean", "StockCode_clean", "CustomerID_clean",
            "date_id", "InvoiceDate_utc", "Country_clean", "transaction_type",
            "Quantity_int", "UnitPrice_num", "gross_amount", "signed_amount", "source_file"
        ]

        for start_idx in range(0, total_valid, batch_size):
            chunk = df_valid.iloc[start_idx:start_idx + batch_size][fact_cols]
            fact_chunk_rows = [
                (
                    str(r[0])[:64],
                    str(r[1])[:50],
                    str(r[2])[:50],
                    str(r[3])[:50],
                    pd.to_datetime(r[4]).date(),
                    pd.to_datetime(r[5]).to_pydatetime(),
                    str(r[6])[:100],
                    str(r[7])[:20],
                    int(r[8]),
                    float(r[9]),
                    float(r[10]),
                    float(r[11]),
                    str(r[12])[:50],
                )
                for r in chunk.itertuples(index=False, name=None)
            ]
            execute_values(cur, insert_fact_sql, fact_chunk_rows, page_size=batch_size)

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
            cols_map = ["record_hash", "InvoiceNo", "StockCode", "Description", "Quantity", "UnitPrice", "InvoiceDate", "CustomerID", "Country", "source_file", "rejection_reason"]
            for c in cols_map:
                if c not in df_rejected.columns:
                    df_rejected[c] = ""
            df_rej_clean = df_rejected[cols_map].fillna("")
            rej_rows = [
                (
                    str(r[0])[:64],
                    str(r[1])[:50],
                    str(r[2])[:50],
                    str(r[3]),
                    str(r[4])[:50],
                    str(r[5])[:50],
                    str(r[6])[:50],
                    str(r[7])[:50],
                    str(r[8])[:100],
                    str(r[9])[:50],
                    str(r[10])[:255],
                )
                for r in df_rej_clean.itertuples(index=False, name=None)
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
    print("Pipeline ETL finalizado e idempotente.")

with DAG(
    dag_id="datamart_etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL Unificado de DataMart S.A.S.",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datamart", "etl", "master"],
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
        task_id="transform_and_validate",
        python_callable=transform_and_validate,
    )

    t_load = PythonOperator(
        task_id="load_to_postgres_dwh",
        python_callable=load_to_dwh,
    )

    [t_extract_daily, t_extract_hist] >> t_transform >> t_load
