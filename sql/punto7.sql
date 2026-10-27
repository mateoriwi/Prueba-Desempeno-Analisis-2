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