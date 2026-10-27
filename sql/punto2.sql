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