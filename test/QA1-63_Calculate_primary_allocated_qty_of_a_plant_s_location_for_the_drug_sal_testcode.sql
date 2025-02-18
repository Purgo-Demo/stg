-- Install necessary libraries if not present
-- Below SQL is primarily for indicating that installation should be manual if libraries were required
-- Databricks SQL typically doesn't require specific library installations like PySpark

/* Test Code for Allocated Quantity Calculation in Databricks */

-- Create view for allocated quantity calculation
CREATE OR REPLACE VIEW purgo_playground.vw_order_allocated_qty AS
SELECT 
  order_nbr,
  order_line_nbr,
  -- Calculate allocated quantity using the formula: primary_qty + open_qty + shipped_qty - cancel_qty
  (primary_qty + open_qty + shipped_qty - cancel_qty) AS allocated_qty
FROM purgo_playground.f_order
WHERE order_nbr IS NOT NULL AND order_line_nbr IS NOT NULL;

-- Schema validation test
-- Ensure vw_order_allocated_qty has the correct schema
DESCRIBE purgo_playground.vw_order_allocated_qty;

-- Select to view a sample of calculated data
SELECT * FROM purgo_playground.vw_order_allocated_qty LIMIT 10;

-- Unit test to validate the calculation logic with expected results
SELECT 
  ft.order_nbr,
  ft.order_line_nbr,
  ft.expected_allocated_qty,
  fv.allocated_qty,
  -- Assert that the calculated allocated_qty matches the expected_allocated_qty
  CASE 
    WHEN ft.expected_allocated_qty IS NOT NULL 
    AND fv.allocated_qty = ft.expected_allocated_qty THEN 'Pass'
    WHEN ft.expected_allocated_qty IS NULL 
    AND fv.allocated_qty IS NULL THEN 'Pass'
    ELSE 'Fail'
  END AS test_result
FROM purgo_playground.f_order_test ft
LEFT JOIN purgo_playground.vw_order_allocated_qty fv
ON ft.order_nbr = fv.order_nbr AND ft.order_line_nbr = fv.order_line_nbr;

-- Validating Delta Lake operations for correct allocation handling if applicable
-- Assuming table purgo_playground.f_order is a Delta table
MERGE INTO purgo_playground.f_order AS target
USING purgo_playground.f_order_test AS source
ON target.order_nbr = source.order_nbr AND target.order_line_nbr = source.order_line_nbr
WHEN MATCHED THEN
  UPDATE SET 
    target.primary_qty = source.primary_qty,
    target.open_qty = source.open_qty,
    target.shipped_qty = source.shipped_qty,
    target.cancel_qty = source.cancel_qty;

-- Clean up operations
DROP VIEW IF EXISTS purgo_playground.vw_order_allocated_qty;

-- Drop the test table to ensure no residual data
DROP TABLE IF EXISTS purgo_playground.f_order_test;

/* Additional Integration and Performance Tests can be added here as needed */
