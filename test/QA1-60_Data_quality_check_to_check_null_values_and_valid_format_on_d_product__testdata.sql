-- Install necessary libraries if needed
-- Use only Databricks native data types

-- Create test data for the purgo_playground.d_product table

-- Happy path test data
CREATE TABLE purgo_playground.d_product_test_happy AS
SELECT
  'prod_001' AS prod_id,
  'item_001' AS item_nbr,
  50.0 AS unit_cost,
  20230321 AS prod_exp_dt, -- correct YYYYMMDD format
  5.0 AS cost_per_pkg,
  'Address 001' AS plant_add,
  'LOC1' AS plant_loc_cd,
  'Line A' AS prod_line,
  'STK' AS stock_type,
  30.0 AS pre_prod_days,
  100.0 AS sellable_qty,
  'ord_trk_01' AS prod_ordr_tracker_nbr,
  'max_ord_5' AS max_order_qty,
  'Y' AS flag_active,
  '2024-03-21T00:00:00.000+0000' AS crt_dt, -- correct timestamp format
  '2024-03-21T12:00:00.000+0000' AS updt_dt, -- correct timestamp format
  'SRC_SYS' AS src_sys_cd,
  'Entity_1' AS hfm_entity
UNION ALL
SELECT
  'prod_002',
  'item_002',
  60.5,
  20230422, -- correct YYYYMMDD format
  7.0,
  'Address 002',
  'LOC2',
  'Line B',
  'STK',
  45.0,
  200.0,
  'ord_trk_02',
  'max_ord_10',
  'Y',
  '2024-04-21T00:00:00.000+0000',
  '2024-04-21T12:00:00.000+0000',
  'SRC_SYS',
  'Entity_2';

-- NULL handling scenarios and special characters
INSERT INTO purgo_playground.d_product_test_happy
SELECT
  'prod_003',
  NULL, -- item_nbr is null
  80.0,
  20230520, -- correct YYYYMMDD format
  12.0,
  NULL, -- plant_add is NULL
  NULL, -- plant_loc_cd is NULL
  NULL, -- prod_line is NULL
  'CON', -- special stock_type with special characters
  60.0,
  NULL, -- sellable_qty is null
  NULL, -- prod_ordr_tracker_nbr is NULL
  NULL, -- max_order_qty is NULL
  'N',
  NULL, -- crt_dt is NULL
  NULL, -- updt_dt is NULL
  'SRC_SYS',
  'Entity_3';

-- Edge cases, with out-of-range and non-standard values
INSERT INTO purgo_playground.d_product_test_happy
SELECT
  'prod_004',
  'item_004',
  -20.0, -- negative unit cost (edge case)
  1234567, -- non-standard date format
  -5.0, -- negative cost per pkg (edge case)
  'Address 004',
  'LOC4',
  'Line D',
  'FRE', -- special characters in stock_type
  -30.0, -- negative pre_prod_days (edge case)
  -10.0, -- negative sellable_qty (edge case)
  'special_ord_trk_@!$', -- special characters in tracker
  'max_ord_50',
  'N',
  '2020-02-29T00:00:00.000+0000', -- leap day test for crt_dt
  '2020-02-29T12:00:00.000+0000',
  'SRC_SYS_#@!',
  'Entity_4';

-- SQL logic for data quality check

-- Check if 'item_nbr' is not null and return count and sample records
WITH item_nbr_null AS (
  SELECT * FROM purgo_playground.d_product WHERE item_nbr IS NULL
)
SELECT COUNT(*) AS null_item_nbr_count FROM item_nbr_null;
SELECT * FROM item_nbr_null LIMIT 5;

-- Check if 'sellable_qty' is not null and return count and sample records
WITH sellable_qty_null AS (
  SELECT * FROM purgo_playground.d_product WHERE sellable_qty IS NULL
)
SELECT COUNT(*) AS null_sellable_qty_count FROM sellable_qty_null;
SELECT * FROM sellable_qty_null LIMIT 5;

-- Check if 'prod_exp_dt' is in the correct format YYYYMMDD and return count and sample records
WITH prod_exp_dt_invalid AS (
  SELECT * FROM purgo_playground.d_product WHERE CAST(prod_exp_dt AS STRING) NOT REGEXP '^[0-9]{8}$'
)
SELECT COUNT(*) AS invalid_prod_exp_dt_count FROM prod_exp_dt_invalid;
SELECT * FROM prod_exp_dt_invalid LIMIT 5;
