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