-- Ensure necessary configurations and clean up pre-existing objects
-- This code snippet performs required table alterations and validations in Databricks using Databricks SQL

/* Step 1: Drop existing clone tables and views if they exist to ensure a clean setup */
DROP TABLE IF EXISTS purgo_playground.d_product_clone;
DROP VIEW IF EXISTS purgo_playground.d_product_vw_clone;

/* Step 2: Create a clone of the d_product table adding 'source_country' column with default 'NL' */
CREATE TABLE purgo_playground.d_product_clone AS
SELECT *, 'NL' AS source_country
FROM purgo_playground.d_product;

/* Validate addition of 'source_country' and ensure all values are 'NL' */
WITH validation_cte AS (
  SELECT COUNT(*) AS cnt_mismatch 
  FROM purgo_playground.d_product_clone 
  WHERE source_country != 'NL'
)
SELECT CASE WHEN cnt_mismatch = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
       cnt_mismatch AS mismatch_count 
FROM validation_cte;

/* Confirm the 'source_country' column's presence in the schema */
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

/* Step 3: Alter the view to incorporate the new 'source_country' column */
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

/* Validate that the view includes the source_country and all values are 'NL' */
WITH view_validation_cte AS (
  SELECT COUNT(*) AS cnt_vw_mismatch 
  FROM purgo_playground.d_product_vw_clone 
  WHERE source_country != 'NL'
)
SELECT CASE WHEN cnt_vw_mismatch = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
       cnt_vw_mismatch AS mismatch_count 
FROM view_validation_cte;

/* Cleanup operations to ensure reversibility of test changes */
DROP TABLE IF EXISTS purgo_playground.d_product_clone;
DROP VIEW IF EXISTS purgo_playground.d_product_vw_clone;

/* Rollback Strategy Placeholder */
/* Implement rollback strategy for production to handle added columns or rows when needed */

/* Integration and performance test placeholders for additional testing not shown in this code */
