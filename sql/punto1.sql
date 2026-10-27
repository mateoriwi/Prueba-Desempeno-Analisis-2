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