CREATE TABLE IF NOT EXISTS stage_raw (
    InvoiceNo TEXT,
    StockCode TEXT,
    Description TEXT,
    Quantity INTEGER,
    InvoiceDate TIMESTAMP,
    UnitPrice NUMERIC,
    CustomerID TEXT,
    Country TEXT
);


CREATE TABLE IF NOT EXISTS customer(
    customer_key INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY, 
    CustomerID TEXT UNIQUE NOT NULL,
    country TEXT
);

CREATE TABLE IF NOT EXISTS product(
    product_key INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY,
    StockCode TEXT UNIQUE NOT NULL,
    Description TEXT
);


CREATE TABLE IF NOT EXISTS fact_sales (
    InvoiceNo TEXT,
    InvoiceDate Timestamp,
    product_key INT,
    customer_key INT,
    quantity INT,
    unit_price NUMERIC(10, 2),
    last_update TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (product_key) REFERENCES product(product_key),
    FOREIGN KEY (customer_key) REFERENCES customer(customer_key)
);

CREATE TABLE IF NOT EXISTS fact_returns (
    InvoiceNo TEXT,
    InvoiceDate Timestamp,
    product_key INT,
    customer_key INT,
    quantity INT,
    unit_price NUMERIC(10, 2),
    last_update TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (product_key) REFERENCES product(product_key),
    FOREIGN KEY (customer_key) REFERENCES customer(customer_key)
);