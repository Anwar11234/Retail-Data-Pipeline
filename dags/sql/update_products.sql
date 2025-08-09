WITH latest_product_records AS (
    SELECT 
        StockCode::TEXT AS StockCode,
        Description,
        ROW_NUMBER() OVER (
        PARTITION BY StockCode
        ORDER BY InvoiceDate DESC
        ) AS rn
    FROM stage_raw
    WHERE StockCode IS NOT NULL
)

INSERT INTO product (StockCode, Description)
SELECT StockCode::TEXT AS StockCode, Description
FROM latest_product_records
WHERE rn = 1
ON CONFLICT (StockCode) DO UPDATE SET
    Description = EXCLUDED.Description;