# 🛒 DataMart S.A.S. — Pipeline ETL con Apache Airflow y Data Warehouse

Solución de ingeniería de datos contenerizada de extremo a extremo (*Zero-Touch Deployment*) que orquesta mediante Apache Airflow la extracción de datasets transaccionales heterogéneos y una API REST de catálogo legado, aplica reglas de limpieza y separación de devoluciones, y carga los datos de forma idempotente en un Data Warehouse en PostgreSQL (Esquema en Estrella).

---

## 📋 Tabla de Contenidos
1. [Arquitectura del Sistema](#-arquitectura-del-sistema)
2. [Estructura del Repositorio](#-estructura-del-repositorio)
3. [Guía de Despliegue Rápido (<10 minutos)](#-guía-de-despliegue-rápido-10-minutos)
4. [Verificación de Connections y Variables](#-verificación-de-connections-y-variables)
5. [Ejecución del Pipeline y Validación](#-ejecución-del-pipeline-y-validación)
6. [Documento de Decisiones Técnicas](#-documento-de-decisiones-técnicas)
   - [Diseño del Modelo Dimensional (Data Warehouse)](#1-diseño-del-modelo-dimensional-data-warehouse)
   - [Resolución de los 3 Casos Ambiguos](#2-resolución-de-los-3-casos-ambiguos)
   - [Garantía de Idempotencia](#3-garantía-de-idempotencia)
7. [Diagrama Entidad-Relación (dbdiagram.io)](#-diagrama-entidad-relación-dbdiagramio)
8. [Consultas SQL para Preguntas de Negocio](#-consultas-sql-para-preguntas-de-negocio)

---

## 🏗️ Arquitectura del Sistema

```
                         ┌────────────────────────────────────────────────────────┐
                         │                 Docker Compose Stack                   │
                         │                                                        │
┌──────────────────┐     │   ┌────────────────────┐    ┌──────────────────────┐   │
│ Fuentes de Datos │     │   │  postgres-airflow  │    │     postgres-dwh     │   │
│ - data.csv       │────►│   │  (Metadatos :5432) │    │ (DWH Analítico :5433)│   │
│ - online_retail  │     │   └─────────▲──────────┘    └──────────▲───────────┘   │
└──────────────────┘     │             │                          │               │
                         │   ┌─────────┴──────────┐               │               │
┌──────────────────┐     │   │    airflow-init    │───────────────┘               │
│   products-api   │────►│   │ (Setup Zero-Touch) │ (init_dwh.sql DDL automático) │
│ (FastAPI :8000)  │     │   └─────────┬──────────┘                               │
└──────────────────┘     │             │                                          │
                         │   ┌─────────▼──────────┐    ┌──────────────────────┐   │
                         │   │ airflow-webserver  │    │  airflow-scheduler   │   │
                         │   │      (:8080)       │    │   (LocalExecutor)    │   │
                         │   └────────────────────┘    └──────────────────────┘   │
                         └────────────────────────────────────────────────────────┘
```

- **Airflow Core (`LocalExecutor`):** Orquestador ligero y eficiente sin dependencia de Redis ni Celery Workers.
- **Doble Instancia PostgreSQL:** Separación estricta de base de datos de metadatos de Airflow (`postgres-airflow`) y el Data Warehouse analítico de producción (`postgres-dwh`).
- **Zero-Touch Deployment:** Contenedor efímero `airflow-init` que migra la BD, crea el usuario `admin`, y auto-registra las conexiones y variables antes de encender el scheduler y webserver.

---

## 📁 Estructura del Repositorio

```text
.
├── .env.example                 # Plantilla de variables de entorno sin secretos
├── .gitignore                   # Excluye .env, logs/, __pycache__ y temporales
├── docker-compose.yml           # Orquestación multicontenedor (Airflow + 2 Postgres + FastAPI)
├── README.md                    # Guía integral de despliegue y decisiones técnicas
├── api/                         # Microservicio de catálogo legado (FastAPI - Bono 4.3)
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
├── dags/                        # Definición de tuberías en Airflow
│   ├── 01_extract_dag.py        # Fase 1: Extracción y staging intermedio
│   ├── 02_transform_dag.py      # Fase 2: Limpieza, API y exportación a data/clean/
│   ├── 03_load_dag.py           # Fase 3: Carga idempotente en PostgreSQL DWH
│   └── datamart_etl_dag.py      # Pipeline maestro unificado
├── data/                        # Volumen compartido y Data Lake local
│   ├── raw/                     # Capa bronce: Datasets transaccionales originales
│   │   ├── data.csv             # Fuente 1: Transacciones diarias
│   │   └── Sales Transaction v.4a.csv # Fuente 2: Historial extendido
│   └── clean/                   # Capa plata: Datos normalizados generados por Airflow
│       ├── transacciones_limpias.csv
│       ├── productos_catalogo_limpio.csv
│       └── registros_rechazados.csv
└── sql/
    ├── init_dwh.sql             # Script DDL montado en /docker-entrypoint-initdb.d/
    ├── business_questions.sql   # Script consolidado para las 7 preguntas de negocio
    └── punto1.sql - punto7.sql  # Consultas SQL individuales por pregunta
```

> **📌 Nota de Ingeniería sobre `data/clean/`:**  
> Siguiendo las mejores prácticas de Data Engineering, los archivos procesados dentro de `data/clean/` están excluidos del control de versiones mediante `.gitignore` (manteniendo únicamente su estructura de carpetas vía `.gitkeep`). Esto evita inflar el repositorio Git con datos derivados y garantiza que cada ejecución del pipeline o del DAG de transformación (`02_transform_dag.py`) genere los artefactos limpios de forma reproducible y desacoplada del código fuente.

---

## 🚀 Guía de Despliegue Rápido (<10 minutos)

### 1. Clonar el repositorio y acceder a la carpeta
```bash
git clone <URL_DEL_REPOSITORIO>
cd <CARPETA_DEL_REPOSITORIO>
```

### 2. Configurar variables de entorno
Copia la plantilla `.env.example` a `.env`:
```bash
cp .env.example .env
```
*(En Windows PowerShell: `Copy-Item .env.example .env`)*

### 3. Verificar datasets
Asegúrate de que los archivos transaccionales se encuentren en la carpeta `./data/raw/` (o `./data/`):
- `./data/raw/data.csv`
- `./data/raw/online_retail_II.csv` (o `Sales Transaction v.4a.csv`)

### 4. Levantar la plataforma con un solo comando
```bash
docker compose up -d --build
```

---

## 🔍 Verificación de Connections y Variables

El stack inicializa de forma 100% automática las conexiones y variables requeridas por el enunciado. Puedes verificarlo sin ingresar a la UI:

### Conexiones de Airflow:
```bash
docker exec -it datamart_airflow_webserver airflow connections list
```
*Salida esperada:* Conexión `datamart_dwh_conn` (Postgres) y `products_api_conn` (HTTP).

### Variables de Airflow:
```bash
docker exec -it datamart_airflow_webserver airflow variables list
```
*Salida esperada:* Variables `datamart_data_dir`, `datamart_batch_size` y `datamart_api_url`.

---

## ⚡ Ejecución del Pipeline y Validación

1. Ingresa a la interfaz web de Airflow en tu navegador:
   - **URL:** [http://localhost:8080](http://localhost:8080)
   - **Usuario:** `admin`
   - **Contraseña:** `admin_datamart_2026` *(o la definida en tu `.env`)*
2. Enciende el switch (toggle) del DAG **`datamart_etl_pipeline`** y presiona el botón **Trigger DAG** (ícono de play).
3. Alternativamente, ejecuta el DAG desde la consola:
   ```bash
   docker exec -it datamart_airflow_webserver airflow dags trigger datamart_etl_pipeline
   ```
4. **Validar la carga en PostgreSQL DWH:**
   ```bash
   docker exec -it datamart_postgres_dwh psql -U dwh_admin -d datamart_dwh -c "SELECT transaction_type, COUNT(*), ROUND(SUM(gross_amount), 2) AS total_gross, ROUND(SUM(signed_amount), 2) AS total_net FROM fact_transactions GROUP BY transaction_type;"
   ```

---

## 🧠 Documento de Decisiones Técnicas

### 1. Diseño del Modelo Dimensional (Data Warehouse)
Se implementó un **Esquema en Estrella (Star Schema)** en la base de datos analítica `datamart_dwh`:
- **`dim_date`:** Facilita agregaciones temporales sin sobrecargar el motor con conversiones dinámicas.
- **`dim_product`:** Centraliza descripciones canónicas, categorías oficiales, país de origen del proveedor y el conteo histórico de variaciones de nombres (`raw_description_variants_count`).
- **`dim_customer`:** Segmenta compras identificadas (`is_identified = TRUE`) de transacciones no identificadas (`is_identified = FALSE`, `customer_id = 'GUEST'`).
- **`fact_transactions`:** Tabla granular de hechos transaccionales que separa ventas (`SALE`) de devoluciones (`RETURN`), registrando el monto bruto (`gross_amount`) y el importe neto con signo (`signed_amount`).
- **`fact_daily_product_summary`:** Tabla agregada por fecha y producto que pre-calcula ventas brutas, devoluciones y ventas netas, permitiendo resolver preguntas analíticas con latencia inferior a 50 milisegundos.
- **`audit_rejected_records`:** Repositorio de trazabilidad que almacena registros corruptos con su motivo de rechazo (precios $\le 0$, fechas inválidas).

### 2. Resolución de los 3 Casos Ambiguos

| Caso Ambiguo | Decisión Técnica Adoptada | Justificación Analítica de Negocio |
| :--- | :--- | :--- |
| **1. Transacciones sin `CustomerID`** | Incluirlas en el DWH asignando el valor `'GUEST'` y marcando `is_identified = FALSE`. | Las compras anónimas (*Guest Checkout*) representan más del 20% del volumen transaccional en e-commerce. Descartarlas distorsionaría el cierre contable de finanzas y ocultaría ventas legítimas. |
| **2. Variaciones en `Description` para un mismo código** | Normalizar a mayúsculas (`UPPER(TRIM())`) y seleccionar la **moda estadística** como nombre canónico, registrando el total de variantes en la dimensión. | Previene que errores tipográficos aislados corrompan el catálogo oficial y permite auditar la calidad histórica del catálogo para responder la pregunta 6. |
| **3. Solapamiento de fechas y duplicados entre CSVs** | Homologar esquemas y deduplicar mediante clave natural compuesta: `(InvoiceNo, StockCode, Quantity, UnitPrice, InvoiceDate)`, otorgando prioridad a la fuente diaria (`data.csv`). | Ambas fuentes contienen solapamiento temporal. La fuente diaria representa el volcado de producción más reciente y confiable, por lo que ante colisión prevalece sobre el histórico. |

### 3. Garantía de Idempotencia
La idempotencia es total frente a múltiples ejecuciones:
- **Hashing Determinista:** Se genera una clave subrogada SHA-256 (`transaction_hash`) por cada transacción a partir de su clave natural de negocio.
- **Estrategia Upsert (`ON CONFLICT DO UPDATE`):** Las inserciones en las tablas de hechos y dimensiones actualizan los campos en caso de colisión en lugar de duplicar filas. Si el DAG se ejecuta 1, 2 o 5 veces en el mismo día, el conteo de filas y los totales financieros permanecen exactamente iguales.

---

## 📊 Diagrama Entidad-Relación (dbdiagram.io)

El diagrama visual exportado se encuentra disponible en [`modelo_entidad_relacion.pdf`](./modelo_entidad_relacion.pdf).

Código DBML listo para editar o visualizar en [dbdiagram.io](https://dbdiagram.io/d):

```sql
Table dim_date {
  date_id date [pk]
  year int
  month int
  year_month varchar(7)
  day_of_week int
  is_weekend boolean
}

Table dim_product {
  stock_code varchar(50) [pk]
  canonical_description varchar(255)
  category varchar(50)
  supplier_country varchar(50)
  is_active boolean
  raw_description_variants_count int
  updated_at timestamp
}

Table dim_customer {
  customer_id varchar(50) [pk]
  is_identified boolean
  first_seen_country varchar(100)
}

Table fact_transactions {
  transaction_hash varchar(64) [pk]
  invoice_no varchar(50)
  stock_code varchar(50) [ref: > dim_product.stock_code]
  customer_id varchar(50) [ref: > dim_customer.customer_id]
  date_id date [ref: > dim_date.date_id]
  invoice_timestamp_utc timestamp
  country varchar(100)
  transaction_type varchar(20)
  quantity int
  unit_price numeric(12, 2)
  gross_amount numeric(14, 2)
  signed_amount numeric(14, 2)
  source_file varchar(50)
  loaded_at timestamp
}

Table fact_daily_product_summary {
  summary_id varchar(120) [pk]
  date_id date [ref: > dim_date.date_id]
  stock_code varchar(50) [ref: > dim_product.stock_code]
  sales_quantity int
  returns_quantity int
  gross_revenue numeric(14, 2)
  returns_revenue numeric(14, 2)
  net_revenue numeric(14, 2)
  updated_at timestamp
}

Table audit_rejected_records {
  reject_id serial [pk]
  record_hash varchar(64) [unique]
  raw_invoice_no varchar(50)
  raw_stock_code varchar(50)
  raw_description text
  raw_quantity varchar(50)
  raw_unit_price varchar(50)
  raw_invoice_date varchar(50)
  raw_customer_id varchar(50)
  raw_country varchar(100)
  source_file varchar(50)
  rejection_reason varchar(255)
  rejected_at timestamp
}
```

---

## 📈 Consultas SQL para Preguntas de Negocio

El script completo se encuentra en [`sql/business_questions.sql`](file:///sql/business_questions.sql). Para ejecutar todas las consultas directamente en el contenedor del DWH:

```bash
docker exec -i datamart_postgres_dwh psql -U dwh_admin -d datamart_dwh < sql/business_questions.sql
```

### Resumen de Preguntas Cubiertas:
1. **Evolución mensual de ventas netas:** Agrupación por `year_month` con ingreso bruto, devoluciones y tasa de retorno porcentual.
2. **Categorías con mayor revenue y retorno:** Ranking de las 5 categorías oficiales por volumen facturado y porcentaje de devoluciones.
3. **Top 10 productos:** Mayor ingreso neto y Top 10 con mayor tasa de devolución (con filtro de significancia estadística $\ge 100$ unidades).
4. **Desempeño geográfico:** Concentración de órdenes por país y variación del ticket promedio por orden.
5. **Segmentación de clientes:** Comparación del ticket promedio y unidades por compra entre clientes identificados vs. invitados (`GUEST`).
6. **Inconsistencias de catálogo:** Conteo total de códigos únicos y auditoría de referencias con descripciones variantes.
7. **Recomendación para Producto:** Hallazgo analítico de devoluciones atípicas por digitación en compras mayoristas (>1.000 unidades) y cargos manuales (`M`, `POST`), con propuesta de blindaje en ERP.
