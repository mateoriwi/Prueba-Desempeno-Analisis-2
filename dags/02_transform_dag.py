from datetime import datetime, timedelta
import hashlib
import os
import pandas as pd
import requests
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

def transform_and_validate(**context):
    """Aplica reglas de negocio, resuelve casos ambiguos, exporta a data/clean/ y prepara para carga."""
    data_dir = Variable.get("datamart_data_dir", default_var="/opt/airflow/data")
    staging_dir = os.path.join(data_dir, ".staging")
    os.makedirs(staging_dir, exist_ok=True)
    clean_dir = os.path.join(data_dir, "clean")
    os.makedirs(clean_dir, exist_ok=True)

    daily_path = os.path.join(staging_dir, "raw_daily.parquet")
    hist_path = os.path.join(staging_dir, "raw_historical.parquet")

    # Obtener URL de la API desde Connection o Variable
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

    # 1. Prioridad a Fuente 1 (volcado operacional vigente)
    df_all = pd.concat([df_daily, df_hist], ignore_index=True)

    # Normalización de strings
    df_all["StockCode_clean"] = df_all["StockCode"].fillna("").astype(str).str.strip().str.upper()
    df_all["InvoiceNo_clean"] = df_all["InvoiceNo"].fillna("").astype(str).str.strip().str.upper()
    df_all["Description_clean"] = (
        df_all["Description"].fillna("UNKNOWN PRODUCT").astype(str).str.strip().str.upper()
        .str.replace(r"\s+", " ", regex=True)
    )
    df_all["Country_clean"] = df_all["Country"].fillna("UNKNOWN").astype(str).str.strip().str.upper()

    # Conversión numérica y fechas estandarizadas a UTC (con soporte de formatos mixtos)
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
    print(f"Registros rechazados auditados: {len(df_rejected)}")

    # --- 3. REGISTROS VÁLIDOS Y REGLAS DE NEGOCIO ---
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

    # Enriquecimiento vía API de catálogo (con prueba de salud previa y fallback determinista)
    session = requests.Session()
    api_available = False
    try:
        health_resp = session.get(f"{api_url}/health", timeout=2.0)
        if health_resp.status_code == 200:
            api_available = True
            print("Microservicio API de catálogo activo y respondiendo.")
    except Exception as e:
        print(f"Aviso: API de catálogo no responde ({e}). Activando fallback determinista de clasificación.")

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

    # Guarda staging interno Parquet para el DAG de carga
    df_products.to_parquet(os.path.join(staging_dir, "dim_products.parquet"), index=False)
    df_valid.to_parquet(os.path.join(staging_dir, "valid_transactions.parquet"), index=False)

    # 4. EXPORTACIÓN A DATA LAKE LOCAL (data/clean/):
    df_valid.to_csv(os.path.join(clean_dir, "transacciones_limpias.csv"), index=False)
    df_products.to_csv(os.path.join(clean_dir, "productos_catalogo_limpio.csv"), index=False)
    print(f"Exportación local completada con éxito en: {clean_dir} ({len(df_valid)} transacciones limpias)")

with DAG(
    dag_id="datamart_02_transform",
    default_args=default_args,
    description="Fase 2: Limpieza, enriquecimiento y guardado en data/clean/",
    schedule_interval=None,  # Orquestado vía Trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datamart", "etl", "transform"],
) as dag:

    t_transform = PythonOperator(
        task_id="transform_clean_and_export",
        python_callable=transform_and_validate,
    )

    t_trigger_load = TriggerDagRunOperator(
        task_id="trigger_load_dag",
        trigger_dag_id="datamart_03_load",
        wait_for_completion=False,
    )

    t_transform >> t_trigger_load
