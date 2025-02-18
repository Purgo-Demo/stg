-- SQL test code for Databricks environment
-- Ensure the Unity Catalog Schema is set up correctly

-- Test for Schema Validation

-- Schema Test for d_product_vw view to check if the columns and types are correct
CREATE OR REPLACE TEMP VIEW test_view_schema AS
SELECT
  'prod_id' AS COLUMN_NAME, 'STRING' AS DATA_TYPE
UNION ALL SELECT
  'item_nbr', 'STRING'
UNION ALL SELECT
  'unit_cost', 'DOUBLE'
UNION ALL SELECT
  'prod_exp_dt', 'DECIMAL(38,0)'
UNION ALL SELECT
  'cost_per_pkg', 'DOUBLE'
UNION ALL SELECT
  'plant_add', 'STRING'
UNION ALL SELECT
  'plant_loc_cd', 'STRING'
UNION ALL SELECT
  'prod_line', 'STRING'
UNION ALL SELECT
  'stock_type', 'STRING'
UNION ALL SELECT
  'pre_prod_days', 'DOUBLE'
UNION ALL SELECT
  'sellable_qty', 'DOUBLE'
UNION ALL SELECT
  'prod_ordr_tracker_nbr', 'STRING'
UNION ALL SELECT
  'max_order_qty', 'STRING'
UNION ALL SELECT
  'flag_active', 'STRING'
UNION ALL SELECT
  'crt_dt', 'TIMESTAMP'
UNION ALL SELECT
  'updt_dt', 'TIMESTAMP'
UNION ALL SELECT
  'hfm_entity', 'STRING';

-- Assert if schema matches expected
SELECT * FROM (
  SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns
  WHERE table_schema = 'purgo_playground' AND table_name = 'd_product_vw'
  EXCEPT
  SELECT COLUMN_NAME, DATA_TYPE FROM test_view_schema
) AS differences;

-- Test Case 1: Verify the proper update of hfm_entity for src_sys_cd = 'orafin'
WITH expected_vw AS (
  SELECT
    dp.prod_id,
    dp.item_nbr,
    dp.unit_cost,
    dp.prod_exp_dt,
    dp.cost_per_pkg,
    dp.plant_add,
    dp.plant_loc_cd,
    dp.prod_line,
    dp.stock_type,
    dp.pre_prod_days,
    dp.sellable_qty,
    dp.prod_ordr_tracker_nbr,
    dp.max_order_qty,
    dp.flag_active,
    dp.crt_dt,
    dp.updt_dt,
    CASE WHEN dp.src_sys_cd = 'orafin' THEN el.lkkup_val_01 ELSE dp.hfm_entity END AS hfm_entity
  FROM purgo_playground.d_product_test_scenarios dp
  LEFT JOIN purgo_playground.edp_lkup el
  ON dp.stock_type = el.lkup_key_01
)
SELECT * FROM (
  SELECT COUNT(*) AS mismatched_rows
  FROM (
    SELECT * FROM purgo_playground.d_product_vw
    EXCEPT 
    SELECT * FROM expected_vw
  ) AS test_diff
) WHERE mismatched_rows = 0;

-- Test Case 2: Ensure no change in hfm_entity for non-'orafin' src_sys_cd
WITH unchanged_entity AS (
  SELECT dp.prod_id FROM purgo_playground.d_product_test_scenarios dp
  LEFT JOIN purgo_playground.d_product_vw vw 
  ON dp.prod_id = vw.prod_id
  WHERE dp.src_sys_cd != 'orafin' AND dp.hfm_entity = vw.hfm_entity
)
SELECT COUNT(*) AS unchanged_count
FROM unchanged_entity
WHERE unchanged_count = (SELECT COUNT(*) FROM purgo_playground.d_product_test_scenarios WHERE src_sys_cd != 'orafin');

-- Test Case 3: Handle and log missing join keys in edp_lkup
WITH missing_keys AS (
  SELECT dp.stock_type
  FROM purgo_playground.d_product_test_scenarios dp
  LEFT JOIN purgo_playground.edp_lkup el
  ON dp.stock_type = el.lkup_key_01
  WHERE el.lkup_key_01 IS NULL
)
SELECT stock_type, COUNT(*) as missing_count
FROM missing_keys;

-- Data Quality Test: Validate that all populated values for hfm_entity are not NULL when they should be updated
SELECT COUNT(*) AS null_records
FROM purgo_playground.d_product_vw
WHERE src_sys_cd = 'orafin' AND hfm_entity IS NULL;

-- Cleanup testing objects
DROP VIEW IF EXISTS test_view_schema;
DROP VIEW IF EXISTS expected_vw;
DROP VIEW IF EXISTS unchanged_entity;
DROP VIEW IF EXISTS missing_keys;

