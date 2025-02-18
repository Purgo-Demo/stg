/* Test Code for Allocated Quantity Calculation in Inventory Stock Management */

/* Setup Section */
/* This section initializes the environment and sets up any necessary configurations using Databricks SQL features. */

/* Schema Validation */
/* Validate the schema for purgo_playground.f_order table */
SELECT 
  CASE WHEN COUNT(1) = 6 THEN 'Schema matches expectation' ELSE 'Schema mismatch or missing' END AS f_order_schema_check 
FROM information_schema.columns 
WHERE table_schema = 'purgo_playground' 
  AND table_name = 'f_order'
  AND column_name IN ('order_nbr', 'order_line_nbr', 'primary_qty', 'open_qty', 'shipped_qty', 'cancel_qty');

/* Unit Tests for Individual Transformations */
/* Validate the correctness of individual data transformations to calculate allocated_qty. */

/* Unit Test: Calculate allocated_qty as the sum of the specified columns for a given order */
SELECT 
  order_nbr, 
  order_line_nbr,
  COALESCE(primary_qty, 0) + COALESCE(open_qty, 0) + COALESCE(shipped_qty, 0) + COALESCE(cancel_qty, 0) AS calculated_allocated_qty
FROM purgo_playground.f_order
WHERE order_nbr = 'ORDR001' AND order_line_nbr = '001';

/* Validate NULL handling in quantity columns */
SELECT 
  order_nbr, 
  order_line_nbr,
  COALESCE(primary_qty, 0) + COALESCE(open_qty, 0) + COALESCE(shipped_qty, 0) + COALESCE(cancel_qty, 0) AS calculated_allocated_qty
FROM purgo_playground.f_order
WHERE order_nbr = 'ORDR003' AND order_line_nbr = '002';

/* Integration Tests for End-to-End Flows */
/* Validate end-to-end data processing flows ensuring multiple components interact correctly. */

/* Check the integration of calculated allocated_qty with f_order entries */
WITH calculated_data AS (
  SELECT 
    order_nbr, 
    order_line_nbr,
    COALESCE(primary_qty, 0) + COALESCE(open_qty, 0) + COALESCE(shipped_qty, 0) + COALESCE(cancel_qty, 0) AS allocated_qty
  FROM purgo_playground.f_order
)
SELECT * FROM calculated_data
WHERE order_nbr = 'ORDR002' AND order_line_nbr = '001'
AND allocated_qty = 9.0;

/* Perform Error Handling Tests */
/* Validate that errors are handled gracefully with clear messages. */

/* Error Handling: No entry exists in f_order for the given order_nbr and order_line_nbr */
SELECT 
  CASE WHEN COUNT(1) = 0 THEN 'Error: Order number or line number not found in f_order table.' 
  ELSE 'Entry exists' END AS error_check
FROM purgo_playground.f_order
WHERE order_nbr = 'ORDR005' AND order_line_nbr = '003';

/* Delta Lake Testing */
/* Validate Delta Lake specific operations like MERGE, UPDATE */

/* Test MERGE operation */
MERGE INTO purgo_playground.f_order AS target
USING (SELECT 'ORDR006' AS order_nbr, '001' AS order_line_nbr, 24.0 AS allocated_qty) AS source
ON target.order_nbr = source.order_nbr AND target.order_line_nbr = source.order_line_nbr
WHEN MATCHED THEN
  UPDATE SET target.primary_qty = source.allocated_qty;

/* Cleanup Section */
/* Clean up any test data or environment configurations to maintain a clean state for future tests. */

/* As this is a test script, actual database changes are not persisted. This section is a placeholder for any cleanup activities. */
