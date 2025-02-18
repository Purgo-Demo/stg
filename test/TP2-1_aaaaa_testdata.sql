-- SQL code to generate test data for Databricks environment

-- Create a table for inventory movements
CREATE TABLE IF NOT EXISTS inventory_movements (
    transaction_id STRING,
    product_id STRING,
    plant_id STRING,
    quantity BIGINT,
    transaction_date TIMESTAMP
);

-- Insert test data into inventory_movements
INSERT INTO inventory_movements VALUES
('txn001', '101', 'A1', 150, TIMESTAMP('2023-10-01T00:00:00.000+0000')),
('txn002', '102', 'B2', 300, TIMESTAMP('2023-10-02T00:00:00.000+0000')),
-- Edge case: Zero quantity
('txn003', '103', 'C3', 0, TIMESTAMP('2023-10-03T00:00:00.000+0000')),
-- Special characters in product_id
('txn004', '10$4', 'D4', 100, TIMESTAMP('2023-10-04T00:00:00.000+0000')),
-- NULL handling scenario
('txn005', '105', 'E5', NULL, TIMESTAMP('2023-10-05T00:00:00.000+0000'));

-- Create a table for sales data
CREATE TABLE IF NOT EXISTS sales_data (
    sale_id STRING,
    product_id STRING,
    plant_id STRING,
    sales_amount DOUBLE,
    sale_date TIMESTAMP
);

-- Insert test data into sales_data
INSERT INTO sales_data VALUES
('sale001', '101', 'A1', 5000.0, TIMESTAMP('2023-10-01T00:00:00.000+0000')),
('sale002', '102', 'B2', 1500.5, TIMESTAMP('2023-10-02T00:00:00.000+0000')),
-- Edge case: Negative sales amount
('sale003', '103', 'C3', -100.0, TIMESTAMP('2023-10-03T00:00:00.000+0000')),
-- Multi-byte characters in product_id
('sale004', '产品105', 'E5', 200.0, TIMESTAMP('2023-10-04T00:00:00.000+0000')),
-- NULL handling scenario
('sale005', '105', NULL, 100.0, TIMESTAMP('2023-10-05T00:00:00.000+0000'));

-- Query to calculate average inventory and sales
-- and compute DOH
WITH avg_inventory AS (
    SELECT
        product_id,
        plant_id,
        AVG(quantity) AS avg_inv
    FROM inventory_movements
    GROUP BY product_id, plant_id
),
avg_daily_sales AS (
    SELECT
        product_id,
        plant_id,
        AVG(sales_amount) / COUNT(DISTINCT sale_date) AS avg_daily_sales
    FROM sales_data
    GROUP BY product_id, plant_id
)
SELECT
    a.product_id,
    a.plant_id,
    a.avg_inv,
    b.avg_daily_sales,
    CASE 
        WHEN b.avg_daily_sales > 0 THEN (a.avg_inv / b.avg_daily_sales) * 365
        ELSE NULL END AS doh
FROM avg_inventory a
JOIN avg_daily_sales b ON a.product_id = b.product_id AND a.plant_id = b.plant_id;

-- Specific error checks
-- Check for missing inventory data
SELECT DISTINCT 
    product_id 
FROM sales_data 
WHERE product_id NOT IN (SELECT DISTINCT product_id FROM inventory_movements);

-- Check for missing sales data
SELECT DISTINCT 
    product_id 
FROM inventory_movements 
WHERE product_id NOT IN (SELECT DISTINCT product_id FROM sales_data);

-- Ensure data integrity, look for NULL and anomalies
SELECT 
    product_id, 
    plant_id, 
    CASE 
        WHEN quantity IS NULL THEN 'Invalid data: NULL quantity' 
        WHEN quantity < 0 THEN 'Invalid data: Negative quantity'
        ELSE 'Valid' END AS data_status
FROM inventory_movements;

SELECT 
    product_id, 
    plant_id, 
    CASE 
        WHEN sales_amount IS NULL THEN 'Invalid data: NULL sales_amount' 
        WHEN sales_amount < 0 THEN 'Invalid data: Negative sales amount'
        ELSE 'Valid' END AS data_status
FROM sales_data;
