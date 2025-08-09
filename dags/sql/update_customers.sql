WITH latest_customer_records AS (
  SELECT 
    CustomerID::TEXT AS CustomerID,
    Country,
    ROW_NUMBER() OVER (
      PARTITION BY CustomerID
      ORDER BY InvoiceDate DESC
    ) AS rn
  FROM stage_raw
  WHERE CustomerID IS NOT NULL
)
INSERT INTO customer (CustomerID, country)
SELECT CustomerID, Country
FROM latest_customer_records
WHERE rn = 1  
ON CONFLICT (CustomerID) DO UPDATE SET
    country = EXCLUDED.country;
