/* Test Code for Databricks SQL Environment */
-- Ensure dependencies: No installation as this uses native SQL
-- Drop the cloned tables and views if they exist
DROP TABLE IF EXISTS purgo_playground.d_product_clone;
DROP VIEW IF EXISTS purgo_playground.d_product_vw_clone;

-- Perform operation: Add 'source_country' column in the replica of d_product (`d_product_clone`)
CREATE TABLE purgo_playground.d_product_clone AS
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
  src_sys_cd,
  hfm_entity,
  'NL' AS source_country -- Add new column with hardcoded value
FROM purgo_playground.d_product;

-- Validate the new column addition
SELECT *
FROM purgo_playground.d_product_clone
WHERE source_country IS NULL; -- Assert that no NULL values exist

-- Create and validate dependent view
CREATE VIEW purgo_playground.d_product_vw_clone AS
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
  source_country -- Should include new column
FROM purgo_playground.d_product_clone;

-- Validate view reflects the new column
SELECT *
FROM purgo_playground.d_product_vw_clone
WHERE source_country != 'NL'; -- Assert that all values are consistent

-- Integration Test: Validate replication in view
WITH validation_cte AS (
  SELECT DISTINCT source_country
  FROM purgo_playground.d_product_vw_clone
)
SELECT COUNT(*)
FROM validation_cte
WHERE source_country != 'NL'; -- Assert only 'NL' exists

-- Clean-up after tests
DROP TABLE IF EXISTS purgo_playground.d_product_clone;
DROP VIEW IF EXISTS purgo_playground.d_product_vw_clone;

