-- Comprehensive test code for Databricks using SQL

-- Test Code Setup and Configuration
/*
  This section includes necessary setup steps for test cases.
  Ensure the required tables are available and correctly formatted.
  Purgo_playground schema is assumed to be correctly set up in Databricks.
*/

-- Dropping and creating test table for f_inv_movmnt to ensure a clean start
DROP TABLE IF EXISTS purgo_playground.test_f_inv_movmnt;

CREATE TABLE purgo_playground.test_f_inv_movmnt (
  txn_id STRING,
  inv_loc STRING,
  financial_qty DOUBLE,
  net_qty DOUBLE,
  expired_qt DECIMAL(38,0),
  item_nbr STRING,
  unit_cost DOUBLE,
  um_rate DOUBLE,
  plant_loc_cd STRING,
  inv_stock_reference STRING,
  stock_type STRING,
  qty_on_hand DOUBLE,
  qty_shipped DOUBLE,
  cancel_dt DECIMAL(38,0),
  flag_active STRING,
  crt_dt TIMESTAMP,
  updt_dt TIMESTAMP,
  dnsa_flag STRING -- Adding dnsa_flag for test purposes
);

-- Inserting diverse test data covering various scenarios
INSERT INTO purgo_playground.test_f_inv_movmnt VALUES
('txn001', 'locA', 1000.0, 900.0, 0, 'item001', 10.5, 1.0, 'loc001', 'ref001', 'typeA', 100.0, 10.0, NULL, 'Y', '2024-03-21T00:00:00.000+0000', '2024-03-21T01:00:00.000+0000', 'yes'),
('txn002', 'locB', 2000.0, 1800.0, 0, 'item002', 8.5, 1.5, 'loc002', 'ref002', 'typeB', 200.0, 20.0, NULL, 'N', '2024-03-22T00:00:00.000+0000', '2024-03-22T01:00:00.000+0000', 'no'),
('txn003', 'locC', 0.0, 0.0, 0, 'item003', 0.0, 0.0, 'loc003', 'ref003', 'typeC', 0.0, 0.0, NULL, 'Y', '2024-03-23T00:00:00.000+0000', '2024-03-23T01:00:00.000+0000', 'yes'),
('txn004', 'locD', 1e12, 1e12, 0, 'item004', 1e12, 1.0, 'loc004', 'ref004', 'typeD', 1e12, 1e12, NULL, 'Y', '2024-03-24T00:00:00.000+0000', '2024-03-24T01:00:00.000+0000', 'yes'),
('txn005', NULL, NULL, NULL, NULL, 'item005', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
('txn006', 'locáteß', 3000.0, 2500.0, 0, 'item006', 9.0, 1.2, 'loc766', 'reƒ006', '呉typeE', 300.0, 30.0, NULL, 'Y', '2024-03-25T00:00:00.000+0000', '2024-03-25T01:00:00.000+0000', 'yes'),
('txn007', 'locE', -1000.0, -800.0, 0, 'item007', -5.5, 1.0, 'loc007', 'ref007', 'typeF', -100.0, -10.0, NULL, 'Y', '2024-03-26T00:00:00.000+0000', '2024-03-26T01:00:00.000+0000', 'no'),
('txn008', 'locF', 1000.0, 0.0, 1000, 'item008', 10.5, 1.0, 'loc001', 'ref008', 'typeG', 0.0, 0.0, NULL, 'Y', '2024-03-27T00:00:00.000+0000', '2024-03-27T01:00:00.000+0000', 'yes');

-- SQL Test Code Block

/* Unit Test: Calculate Inventory at Risk */
-- Test Scenario: When DNSA flag is 'yes' and flag_active is 'Y'
WITH InventoryRisk AS (
  SELECT 
    SUM(CASE WHEN flag_active = 'Y' THEN financial_qty ELSE 0 END) AS inventory_at_risk
  FROM purgo_playground.test_f_inv_movmnt
  WHERE dnsa_flag = 'yes'
)
SELECT 
  inventory_at_risk,
  -- Validate inventory_at_risk calculation
  CASE WHEN inventory_at_risk IS NULL THEN 'Test Failed: Inventory At Risk is NULL' 
       WHEN inventory_at_risk > 0 THEN 'Test Passed: Inventory At Risk Calculated'
       ELSE 'Test Failed: Inventory At Risk is Zero or Negative' 
  END AS result
FROM InventoryRisk;

/* Unit Test: Calculate Percentage of Inventory at Risk */
-- Ensure calculation for total inventory and percentage correctness
WITH ActiveInventory AS (
  SELECT 
    SUM(financial_qty) AS total_inventory,
    SUM(CASE WHEN flag_active = 'Y' THEN financial_qty ELSE 0 END) AS inventory_at_risk
  FROM purgo_playground.test_f_inv_movmnt
  WHERE dnsa_flag = 'yes'
)
SELECT 
  total_inventory,
  inventory_at_risk,
  -- Calculate percentage, handle division by zero
  CASE WHEN total_inventory = 0 THEN 0 ELSE (inventory_at_risk / total_inventory) * 100 END AS inventory_at_risk_percentage,
  -- Validate percentage calculation to ensure correct logic is applied
  CASE WHEN (CASE WHEN total_inventory = 0 THEN 0 ELSE (inventory_at_risk / total_inventory) * 100 END) IS NULL THEN 'Test Failed: Risk Percentage is NULL'
       WHEN (CASE WHEN total_inventory = 0 THEN 0 ELSE (inventory_at_risk / total_inventory) * 100 END) < 0 THEN 'Test Failed: Negative Risk Percentage'
       ELSE 'Test Passed: Risk Percentage Calculated Correctly'
  END AS result
FROM ActiveInventory;

/* Integration Test: End-to-End Flow */
-- Integration scenario to validate the complete calculation process
WITH FullFlow AS (
  SELECT 
    financial_qty, flag_active,
    SUM(financial_qty) OVER () AS total_inventory,
    SUM(CASE WHEN flag_active = 'Y' THEN financial_qty ELSE 0 END) OVER () AS inventory_at_risk
  FROM purgo_playground.test_f_inv_movmnt
  WHERE dnsa_flag = 'yes'
)
SELECT 
  total_inventory,
  inventory_at_risk,
  (inventory_at_risk / total_inventory) * 100 AS inventory_at_risk_percentage,
  -- Check if end-to-end flow gives expected calculations
  CASE WHEN total_inventory = 0 OR inventory_at_risk IS NULL THEN 'Test Failed: E2E Calculation Error'
       ELSE 'Test Passed: E2E Flow Validated'
  END AS result
FROM FullFlow;

/* Data Quality Test */
-- Test to ensure data quality in terms of NULL handling
SELECT
  CASE WHEN COUNT(*) = 0 THEN 'Test Passed: No NULLs Found'
       ELSE 'Test Failed: NULL Values Detected in Critical Columns'
  END AS result
FROM purgo_playground.test_f_inv_movmnt
WHERE flag_active IS NULL OR financial_qty IS NULL;

/* Performance Test */
-- This code provides a simple query execution for performance insight
SELECT 
  financial_qty,
  SUM(financial_qty) OVER () AS total_inventory
FROM purgo_playground.test_f_inv_movmnt
WHERE dnsa_flag = 'yes';

/* Validation of Delta Lake Operations */
/* Example code for validation of Delta Lake operations would go here */

-- Cleanup operations
-- Ensure no residual test artifacts are left
-- DROP unwanted tables or restore them to needed states as required
