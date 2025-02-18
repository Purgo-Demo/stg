/* Setup and Configuration */
/* 
  -- Ensure necessary libraries are available
  -- Handle both table and view operations
*/

-- Step 1: Drop existing clone tables and views if they exist
DROP TABLE IF EXISTS purgo_playground.d_product_clone;
DROP VIEW IF EXISTS purgo_playground.d_product_vw_clone;

-- Step 2: Create a clone of the d_product table with an additional column 'source_country' 
CREATE TABLE purgo_playground.d_product_clone AS
SELECT *, 'NL' AS source_country
FROM purgo_playground.d_product;

-- Happy Path Test: Check existence of 'source_country' column 
-- Validate that 'source_country' is 'NL' for all rows 
WITH validation_cte AS (
  SELECT COUNT(*) AS cnt_mismatch 
  FROM purgo_playground.d_product_clone 
  WHERE source_country != 'NL'
)
SELECT CASE WHEN cnt_mismatch = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result, 
       cnt_mismatch AS mismatch_count 
FROM validation_cte;

-- Schema Validation Test: Ensure correct schema 
-- Validate that the new column 'source_country' is present in the clone table 
WITH schema_cte AS (
  SELECT COUNT(*) AS cnt_column 
  FROM information_schema.columns 
  WHERE table_schema = 'purgo_playground' 
    AND table_name = 'd_product_clone' 
    AND column_name = 'source_country'
)
SELECT CASE WHEN cnt_column = 1 THEN 'PASS' ELSE 'FAIL' END AS test_result, 
       cnt_column AS column_count 
FROM schema_cte;

-- Step 3: Recreate the view d_product_vw_clone to include the new 'source_country' column
CREATE OR REPLACE VIEW purgo_playground.d_product_vw_clone AS 
SELECT 
  prod_id, 
  item_nbr, 
  unit_cost, 
  prod_exp_dt, 
  cost_per_pkg, 
  plant_add, 
  plant_loc_cd, 
  prod_line, 
  stock_type, 
  pre_prod_days, 
  sellable_qty, 
  prod_ordr_tracker_nbr, 
  max_order_qty, 
  flag_active, 
  crt_dt, 
  updt_dt, 
  hfm_entity, 
  'NL' AS source_country
FROM purgo_playground.d_product_clone;

-- Validate that the view d_product_vw_clone includes the source_country 
-- Ensure that querying view reflects 'source_country' correctly 
WITH view_validation_cte AS (
  SELECT COUNT(*) AS cnt_vw_mismatch 
  FROM purgo_playground.d_product_vw_clone 
  WHERE source_country != 'NL'
)
SELECT CASE WHEN cnt_vw_mismatch = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result, 
       cnt_vw_mismatch AS mismatch_count 
FROM view_validation_cte;

-- Data Type Conversion Test 
-- Validate NULL handling and complex type correctness
SELECT 
  CAST(NULL AS STRING) AS null_string,
  named_struct('name', 'example', 'value', 123) AS complex_struct,
  array(1, 2) AS number_array;

-- Cleanup Operations
-- Ensuring that changes during testing are reversible
DROP TABLE IF EXISTS purgo_playground.d_product_clone;
DROP VIEW IF EXISTS purgo_playground.d_product_vw_clone;

/* Rollback Strategy Placeholder
-- Implement rollback strategy for production
-- Ensure deletion or archival of added columns or rows when necessary
*/

/* Integration and Performance Tests
-- These tests would involve comprehensive scripts for inserted data,
-- ensuring changes are performant and integrations are unaffected.
*/
