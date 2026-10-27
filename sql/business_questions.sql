-- =============================================================================
-- DATA WAREHOUSE - DATAMART S.A.S.
-- Consultas SQL de Validación para las 7 Preguntas de Negocio (Sección 7)
-- =============================================================================

\echo '============================================================================='
\echo '1. Evolución mensual de las ventas netas (descontando devoluciones)'
\echo '============================================================================='
SELECT
    d.year_month,
    ROUND(SUM(s.gross_revenue), 2) AS total_gross_revenue,
    ROUND(SUM(s.returns_revenue), 2) AS total_returns_revenue,
    ROUND(SUM(s.net_revenue), 2) AS total_net_revenue,
    ROUND(
        100.0 * SUM(s.returns_revenue) / NULLIF(SUM(s.gross_revenue), 0), 2
    ) AS return_rate_pct
FROM fact_daily_product_summary s
JOIN dim_date d ON s.date_id = d.date_id
GROUP BY d.year_month
ORDER BY d.year_month;

\echo ''
\echo '============================================================================='
\echo '2. Categorías de producto: Revenue bruto y proporción de devoluciones'
\echo '============================================================================='
SELECT
    p.category,
    ROUND(SUM(s.gross_revenue), 2) AS gross_revenue,
    ROUND(SUM(s.returns_revenue), 2) AS returns_revenue,
    ROUND(SUM(s.net_revenue), 2) AS net_revenue,
    ROUND(
        100.0 * SUM(s.returns_revenue) / NULLIF(SUM(s.gross_revenue), 0), 2
    ) AS return_proportion_pct
FROM fact_daily_product_summary s
JOIN dim_product p ON s.stock_code = p.stock_code
GROUP BY p.category
ORDER BY gross_revenue DESC;

\echo ''
\echo '============================================================================='
\echo '3A. Top 10 productos con mayor revenue neto'
\echo '============================================================================='
SELECT
    p.stock_code,
    p.canonical_description,
    p.category,
    ROUND(SUM(s.gross_revenue), 2) AS gross_revenue,
    ROUND(SUM(s.net_revenue), 2) AS net_revenue
FROM fact_daily_product_summary s
JOIN dim_product p ON s.stock_code = p.stock_code
GROUP BY p.stock_code, p.canonical_description, p.category
ORDER BY net_revenue DESC
LIMIT 10;

\echo ''
\echo '============================================================================='
\echo '3B. Top 10 productos con mayor tasa de devolución (mínimo 100 unidades vendidas)'
\echo '============================================================================='
SELECT
    p.stock_code,
    p.canonical_description,
    SUM(s.sales_quantity) AS units_sold,
    SUM(s.returns_quantity) AS units_returned,
    ROUND(
        100.0 * SUM(s.returns_revenue) / NULLIF(SUM(s.gross_revenue), 0), 2
    ) AS return_rate_revenue_pct
FROM fact_daily_product_summary s
JOIN dim_product p ON s.stock_code = p.stock_code
GROUP BY p.stock_code, p.canonical_description
HAVING SUM(s.sales_quantity) >= 100
ORDER BY return_rate_revenue_pct DESC
LIMIT 10;

\echo ''
\echo '============================================================================='
\echo '4. Países con mayor concentración de transacciones y ticket promedio por orden'
\echo '============================================================================='
WITH invoice_totals AS (
    SELECT
        country,
        invoice_no,
        SUM(gross_amount) AS invoice_total
    FROM fact_transactions
    WHERE transaction_type = 'SALE'
    GROUP BY country, invoice_no
)
SELECT
    country,
    COUNT(DISTINCT invoice_no) AS total_orders,
    ROUND(SUM(invoice_total), 2) AS total_sales_revenue,
    ROUND(AVG(invoice_total), 2) AS avg_ticket_per_order
FROM invoice_totals
GROUP BY country
ORDER BY total_orders DESC;

\echo ''
\echo '============================================================================='
\echo '5. Comparación de comportamiento: Clientes Identificados vs Anónimos (GUEST)'
\echo '============================================================================='
WITH order_metrics AS (
    SELECT
        c.is_identified,
        f.invoice_no,
        COUNT(f.stock_code) AS lines_per_order,
        SUM(f.quantity) AS items_per_order,
        SUM(f.gross_amount) AS order_revenue
    FROM fact_transactions f
    JOIN dim_customer c ON f.customer_id = c.customer_id
    WHERE f.transaction_type = 'SALE'
    GROUP BY c.is_identified, f.invoice_no
)
SELECT
    CASE WHEN is_identified THEN 'Identificado (Con CustomerID)' ELSE 'No Identificado (GUEST)' END AS customer_segment,
    COUNT(invoice_no) AS total_orders,
    ROUND(AVG(lines_per_order), 2) AS avg_distinct_lines_per_order,
    ROUND(AVG(items_per_order), 2) AS avg_units_per_order,
    ROUND(AVG(order_revenue), 2) AS avg_order_ticket
FROM order_metrics
GROUP BY is_identified;

\echo ''
\echo '============================================================================='
\echo '6A. Resumen de códigos únicos y productos con descripciones inconsistentes'
\echo '============================================================================='
SELECT
    COUNT(*) AS total_unique_product_codes,
    SUM(CASE WHEN raw_description_variants_count > 1 THEN 1 ELSE 0 END) AS products_with_inconsistent_descriptions
FROM dim_product;

\echo ''
\echo '============================================================================='
\echo '6B. Top 20 productos con mayor número de variaciones de descripción cruda'
\echo '============================================================================='
SELECT
    stock_code,
    canonical_description,
    raw_description_variants_count
FROM dim_product
WHERE raw_description_variants_count > 1
ORDER BY raw_description_variants_count DESC
LIMIT 20;

\echo ''
\echo '============================================================================='
\echo '7. Auditoría de transacciones atípicas y devoluciones anómalas (Soporte recomendación)'
\echo '============================================================================='
SELECT
    f.stock_code,
    p.canonical_description,
    f.invoice_no,
    f.quantity,
    f.unit_price,
    f.gross_amount,
    f.transaction_type,
    f.invoice_timestamp_utc
FROM fact_transactions f
JOIN dim_product p ON f.stock_code = p.stock_code
WHERE f.stock_code IN ('23843', '23166', 'M', 'POST', 'D', 'DOT')
ORDER BY f.gross_amount DESC
LIMIT 20;
