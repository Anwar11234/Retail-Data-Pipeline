INSERT INTO fact_returns (invoiceno, invoicedate, product_key, customer_key, quantity, unit_price, last_update)
SELECT
    r.InvoiceNo,
    r.InvoiceDate,
    p.product_key,
    c.customer_key,
    -1 * r.Quantity, 
    r.UnitPrice, 
    r.InvoiceDate
FROM stage_raw r
JOIN product p ON r.StockCode = p.StockCode
JOIN customer c ON r.CustomerID = c.CustomerID
WHERE r.Quantity < 0 AND r.InvoiceDate > ( 
        SELECT COALESCE(MAX(last_update), '1900-01-01') FROM fact_returns
);