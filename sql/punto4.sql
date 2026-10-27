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