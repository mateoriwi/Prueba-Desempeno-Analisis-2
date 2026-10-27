-- ==========================================================
-- DDL Data Warehouse - DataMart S.A.S.
-- Modelo en Estrella (Star Schema)
-- ==========================================================

-- 1. Dimensión Fecha
CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    year_month VARCHAR(7) NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- 2. Dimensión Producto
CREATE TABLE IF NOT EXISTS dim_product (
    stock_code VARCHAR(50) PRIMARY KEY,
    canonical_description VARCHAR(255) NOT NULL,
    category VARCHAR(50) NOT NULL,
    supplier_country VARCHAR(50) DEFAULT 'Unknown',
    is_active BOOLEAN DEFAULT TRUE,
    raw_description_variants_count INT DEFAULT 1,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Dimensión Cliente
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    is_identified BOOLEAN NOT NULL,
    first_seen_country VARCHAR(100)
);

-- 4. Tabla de Hechos: Transacciones a nivel de línea
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_hash VARCHAR(64) PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    stock_code VARCHAR(50) NOT NULL REFERENCES dim_product(stock_code),
    customer_id VARCHAR(50) NOT NULL REFERENCES dim_customer(customer_id),
    date_id DATE NOT NULL REFERENCES dim_date(date_id),
    invoice_timestamp_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    country VARCHAR(100) NOT NULL,
    transaction_type VARCHAR(20) NOT NULL CHECK (transaction_type IN ('SALE', 'RETURN')),
    quantity INT NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price > 0),
    gross_amount NUMERIC(14, 2) NOT NULL,
    signed_amount NUMERIC(14, 2) NOT NULL,
    source_file VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 5. Tabla de Hechos Agregada: Resumen diario por producto (Alto rendimiento)
CREATE TABLE IF NOT EXISTS fact_daily_product_summary (
    summary_id VARCHAR(120) PRIMARY KEY,
    date_id DATE NOT NULL REFERENCES dim_date(date_id),
    stock_code VARCHAR(50) NOT NULL REFERENCES dim_product(stock_code),
    sales_quantity INT NOT NULL DEFAULT 0,
    returns_quantity INT NOT NULL DEFAULT 0,
    gross_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    returns_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    net_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 6. Tabla de Auditoría: Registros rechazados por reglas de calidad
CREATE TABLE IF NOT EXISTS audit_rejected_records (
    reject_id SERIAL PRIMARY KEY,
    record_hash VARCHAR(64) UNIQUE NOT NULL,
    raw_invoice_no VARCHAR(50),
    raw_stock_code VARCHAR(50),
    raw_description TEXT,
    raw_quantity VARCHAR(50),
    raw_unit_price VARCHAR(50),
    raw_invoice_date VARCHAR(50),
    raw_customer_id VARCHAR(50),
    raw_country VARCHAR(100),
    source_file VARCHAR(50) NOT NULL,
    rejection_reason VARCHAR(255) NOT NULL,
    rejected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Índices de consulta frecuente para optimizar reportes
CREATE INDEX IF NOT EXISTS idx_fact_trans_date ON fact_transactions(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_trans_stock ON fact_transactions(stock_code);
CREATE INDEX IF NOT EXISTS idx_fact_trans_type ON fact_transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_fact_trans_country ON fact_transactions(country);
CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON fact_daily_product_summary(date_id);
