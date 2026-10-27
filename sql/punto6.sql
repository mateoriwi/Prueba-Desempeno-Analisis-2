-- 6A: Resumen de códigos únicos y cuántos presentan inconsistencias
SELECT
    COUNT(*) AS total_unique_product_codes,
    SUM(CASE WHEN raw_description_variants_count > 1 THEN 1 ELSE 0 END) AS products_with_inconsistent_descriptions
FROM dim_product;

-- 6B: Top 20 productos con más variaciones de descripción
SELECT
    stock_code,
    canonical_description,
    raw_description_variants_count
FROM dim_product
WHERE raw_description_variants_count > 1
ORDER BY raw_description_variants_count DESC
LIMIT 20;