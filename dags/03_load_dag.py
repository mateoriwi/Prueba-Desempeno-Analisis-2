"""
DAG 3: Carga Idempotente en Data Warehouse (PostgreSQL)
Caso: DataMart S.A.S.
"""

from datetime import datetime, timedelta
import os
import pandas as pd
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
        # 1. Cargar dim_date (Asegurando tipo datetime.date)
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
        print(f"Cargadas {len(date_rows)} fechas en dim_date.")

        # 2. Cargar dim_product (Con truncamiento a límites VARCHAR y valores limpios)
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
        print(f"Cargados {len(prod_rows)} productos en dim_product.")

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
        print(f"Cargados {len(cust_rows)} clientes en dim_customer.")

        # 4. Cargar fact_transactions por lotes (Evita OOM y maximiza velocidad)
        total_valid = len(df_valid)
        print(f"Iniciando inserción de {total_valid} transacciones en fact_transactions...")

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
            print(f"Progreso fact_transactions: {min(start_idx + batch_size, total_valid)} / {total_valid}")

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
        print("Tabla agregada fact_daily_product_summary precalculada con éxito.")

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
            print(f"Cargados {len(rej_rows)} registros rechazados en audit_rejected_records.")

    conn.commit()
    conn.close()
    print("¡Carga idempotente en PostgreSQL DWH finalizada con éxito!")

with DAG(
    dag_id="datamart_03_load",
    default_args=default_args,
    description="Fase 3: Carga idempotente y modelado dimensional en PostgreSQL",
    schedule_interval=None,  # Orquestado vía Trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datamart", "etl", "load"],
) as dag:

    t_load = PythonOperator(
        task_id="load_to_postgres_dwh",
        python_callable=load_to_dwh,
    )
