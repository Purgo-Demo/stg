-- Create a test database to hold test data
CREATE DATABASE IF NOT EXISTS test_db;

USE test_db;

-- Define the schema for f_inv_movmnt and f_sales datasets
CREATE TABLE IF NOT EXISTS f_inv_movmnt (
  productId BIGINT,
  plantId STRING,
  movementDate TIMESTAMP,
  inventoryLevel BIGINT,
  specialNotes STRING
);

CREATE TABLE IF NOT EXISTS f_sales (
  productId BIGINT,
  plantId STRING,
  saleDate TIMESTAMP,
  unitsSold BIGINT,
  salesRevenue DOUBLE,
  salesNotes STRING
);

-- Generate test data for f_inv_movmnt
INSERT INTO f_inv_movmnt VALUES
  -- Happy path data
  (101, 'A1', TIMESTAMP('2023-01-01T00:00:00.000+0000'), 1500, 'Regular stock update'),
  (102, 'B2', TIMESTAMP('2023-01-01T00:00:00.000+0000'), 2000, 'Initial inventory'),
  -- Edge case: boundary date
  (101, 'A1', TIMESTAMP('2024-12-31T23:59:59.999+0000'), 1800, 'End of year stock'),
  -- Error case: negative inventory
  (102, 'B2', TIMESTAMP('2023-06-15T10:15:00.000+0000'), -50, 'Incorrect entry'),
  -- NULL handling
  (103, 'C3', NULL, NULL, NULL),
  -- Special characters and multi-byte characters
  (104, 'D4', TIMESTAMP('2023-03-21T12:30:45.000+0000'), 500, 'In\u00e9\u00e9 stock'),

-- Generate test data for f_sales
INSERT INTO f_sales VALUES
  -- Happy path data
  (101, 'A1', TIMESTAMP('2023-01-01T00:00:00.000+0000'), 100, 1500.75, 'Regular sale'),
  (102, 'B2', TIMESTAMP('2023-01-01T00:00:00.000+0000'), 200, 3000.50, 'Opening week sale'),
  -- Edge case: boundary date
  (101, 'A1', TIMESTAMP('2024-12-31T23:59:59.999+0000'), 150, 2250.25, 'Year-end sale'),
  -- Error case: excessive sale units
  (102, 'B2', TIMESTAMP('2023-06-15T10:15:00.000+0000'), 100000, 75000.00, 'Bulk sale error'),
  -- NULL handling
  (103, 'C3', NULL, NULL, NULL, NULL),
  -- Special characters and multi-byte characters
  (104, 'D4', TIMESTAMP('2023-03-21T12:30:45.000+0000'), 75, 1125.50, 'Caf\u00e9 sale');

-- Key scenarios for DOH calculation testing
-- DOH Formula: (Average Inventory / Average Daily Sales) * 365

-- Average Inventory Calculation
-- Assuming a simple aggregate query to get average inventory per product and plant

-- Calculate average inventory for product 101 at plant A1
SELECT productId, plantId, AVG(inventoryLevel) AS avg_inventory
FROM f_inv_movmnt
WHERE productId = 101 AND plantId = 'A1'
GROUP BY productId, plantId;

-- Calculate average daily sales for product 101 at plant A1
SELECT productId, plantId, (SUM(unitsSold) / COUNT(DISTINCT saleDate)) AS avg_daily_sales
FROM f_sales
WHERE productId = 101 AND plantId = 'A1'
GROUP BY productId, plantId;

-- Calculate DOH for product 101 at plant A1
SELECT
  f.productId,
  f.plantId,
  (AVG(i.inventoryLevel) / (SUM(f.unitsSold) / COUNT(DISTINCT f.saleDate))) * 365 AS DOH
FROM
  f_sales f
JOIN
  f_inv_movmnt i ON f.productId = i.productId AND f.plantId = i.plantId
WHERE
  f.productId = 101 AND f.plantId = 'A1'
GROUP BY
  f.productId, f.plantId;

-- Error case handling: Inventory data missing
-- Scenario: Missing Inventory data for product 105

CREATE TEMP VIEW missing_inventory AS
SELECT 'Inventory data not available for product ' || STRING(productId) AS error_message
FROM f_sales
WHERE productId = 105 AND NOT EXISTS (
  SELECT 1
  FROM f_inv_movmnt
  WHERE f_inv_movmnt.productId = f_sales.productId
);

-- Error case handling: Sales data missing
-- Scenario: Missing Sales data for product 105

CREATE TEMP VIEW missing_sales AS
SELECT 'Sales data not available for product ' || STRING(productId) AS error_message
FROM f_inv_movmnt
WHERE productId = 105 AND NOT EXISTS (
  SELECT 1
  FROM f_sales
  WHERE f_sales.productId = f_inv_movmnt.productId
);

-- Validate Data Integrity: Scenario for invalid data in f_inv_movmnt
CREATE TEMP VIEW invalid_inventory_data AS
SELECT 'Invalid data in inventory movement dataset for product ' || STRING(productId) || ' at plant ' || plantId AS error_message
FROM f_inv_movmnt
WHERE inventoryLevel < 0;

-- Validate KPI: Checking KPI against business thresholds
-- Assuming a table kpi_thresholds with appropriate rules

CREATE TEMP VIEW doh_threshold_check AS
SELECT
  d.productId,
  d.plantId,
  (AVG(d.inventoryLevel) / (SUM(s.unitsSold) / COUNT(DISTINCT s.saleDate))) * 365 AS DOH,
  CASE
    WHEN (AVG(d.inventoryLevel) / (SUM(s.unitsSold) / COUNT(DISTINCT s.saleDate))) * 365 > 350 THEN 'Good'
    ELSE 'Poor'
  END AS thresholdStatus
FROM
  f_inv_movmnt d
JOIN
  f_sales s ON d.productId = s.productId AND d.plantId = s.plantId
GROUP BY
  d.productId, d.plantId;

-- Security check: Simulate accessing with restricted roles
-- Ensure only users with necessary permissions can access the data, typically checked via Databricks SQL UI and not script
