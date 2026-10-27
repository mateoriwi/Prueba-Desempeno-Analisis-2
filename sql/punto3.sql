-- 3A: Top 10 productos con mayor revenue neto
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

-- 3B: Top 10 productos con mayor tasa de devolución (mínimo 100 unidades vendidas)
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