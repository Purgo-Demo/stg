-- Create or replace a view to calculate the allocated quantity for each order in the f_order table
CREATE OR REPLACE VIEW purgo_playground.vw_order_allocated_qty AS
SELECT 
  order_nbr,
  order_line_nbr,
  -- Calculate allocated quantity using the formula: primary_qty + open_qty + shipped_qty - cancel_qty
  (primary_qty + open_qty + shipped_qty - cancel_qty) AS allocated_qty
FROM purgo_playground.f_order
WHERE order_nbr IS NOT NULL AND order_line_nbr IS NOT NULL;

-- Display the first 10 rows of the view to verify correct calculation
SELECT * FROM purgo_playground.vw_order_allocated_qty LIMIT 10;

-- Unit test to ensure the allocated quantity matches expected results
SELECT 
  ft.order_nbr,
  ft.order_line_nbr,
  ft.expected_allocated_qty,
  fv.allocated_qty,
  -- Test pass if calculated allocated_qty matches expected_allocated_qty
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

-- Perform a merge operation to ensure that the f_order table is correctly updated
MERGE INTO purgo_playground.f_order AS target
USING purgo_playground.f_order_test AS source
ON target.order_nbr = source.order_nbr AND target.order_line_nbr = source.order_line_nbr
WHEN MATCHED THEN
  UPDATE SET 
    target.primary_qty = source.primary_qty,
    target.open_qty = source.open_qty,
    target.shipped_qty = source.shipped_qty,
    target.cancel_qty = source.cancel_qty;

-- Clean-up: Drop the view after use
DROP VIEW IF EXISTS purgo_playground.vw_order_allocated_qty;

-- Drop the test table to clean up test data
DROP TABLE IF EXISTS purgo_playground.f_order_test;

/* 
Code comments and additional logic for Delta Lake operations, error handling,
and performance optimizations should be incorporated in the overall development process.
*/ 
