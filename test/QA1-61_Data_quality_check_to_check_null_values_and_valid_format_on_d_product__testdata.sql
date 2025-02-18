-- Test data generation code for d_product table

-- Create DataFrame with diverse test data
CREATE OR REPLACE TEMP VIEW temp_d_product_test_data AS
SELECT 
  -- Happy path test data
  '1' AS prod_id, 
  '123' AS item_nbr, 
  10.5 AS unit_cost, 
  20230301 AS prod_exp_dt,  -- valid prod_exp_dt in yyyymmdd format
  15.2 AS cost_per_pkg, 
  '123 Main St' AS plant_add, 
  'US001' AS plant_loc_cd, 
  'PL100' AS prod_line, 
  'Retail' AS stock_type, 
  5.0 AS pre_prod_days, 
  100.0 AS sellable_qty, 
  'ORD123' AS prod_ordr_tracker_nbr, 
  '500' AS max_order_qty, 
  'Y' AS flag_active, 
  '2024-03-21T00:00:00.000+0000' AS crt_dt, 
  '2024-03-21T00:00:00.000+0000' AS updt_dt, 
  'SAP' AS src_sys_cd, 
  'Entity001' AS hfm_entity
UNION ALL
-- Edge case with null item_nbr
SELECT 
  '2' AS prod_id, 
  NULL AS item_nbr, 
  15.0 AS unit_cost, 
  20240330 AS prod_exp_dt,  -- valid prod_exp_dt in yyyymmdd format
  20.0 AS cost_per_pkg, 
  '456 Central Ave' AS plant_add, 
  'US002' AS plant_loc_cd, 
  'PL200' AS prod_line, 
  'Wholesale' AS stock_type, 
  7.0 AS pre_prod_days, 
  150.0 AS sellable_qty, 
  'ORD234' AS prod_ordr_tracker_nbr, 
  '600' AS max_order_qty, 
  'N' AS flag_active, 
  '2024-03-21T00:00:00.000+0000' AS crt_dt, 
  '2024-03-21T00:00:00.000+0000' AS updt_dt, 
  'JDE' AS src_sys_cd, 
  'Entity002' AS hfm_entity
UNION ALL
-- Error case with invalid prod_exp_dt format
SELECT 
  '3' AS prod_id, 
  '124' AS item_nbr, 
  20.0 AS unit_cost, 
  99999999 AS prod_exp_dt,  -- invalid prod_exp_dt
  25.0 AS cost_per_pkg, 
  '789 Oak St' AS plant_add, 
  'US003' AS plant_loc_cd, 
  'PL300' AS prod_line, 
  'Export' AS stock_type, 
  10.0 AS pre_prod_days, 
  200.0 AS sellable_qty, 
  'ORD345' AS prod_ordr_tracker_nbr, 
  '700' AS max_order_qty, 
  'Y' AS flag_active, 
  '2024-03-21T00:00:00.000+0000' AS crt_dt, 
  '2024-03-21T00:00:00.000+0000' AS updt_dt, 
  'SAAS' AS src_sys_cd, 
  'Entity003' AS hfm_entity
UNION ALL
-- Special characters
SELECT 
  '4' AS prod_id, 
  '125' AS item_nbr, 
  30.5 AS unit_cost, 
  20250315 AS prod_exp_dt,  -- valid prod_exp_dt in yyyymmdd format
  35.0 AS cost_per_pkg, 
  '101 Maple St!@#' AS plant_add,  -- Special characters in address
  'US004' AS plant_loc_cd, 
  'PL400' AS prod_line, 
  'Import' AS stock_type, 
  12.0 AS pre_prod_days, 
  250.0 AS sellable_qty, 
  'ORD456' AS prod_ordr_tracker_nbr, 
  '800' AS max_order_qty, 
  'N' AS flag_active, 
  '2024-03-21T00:00:00.000+0000' AS crt_dt, 
  '2024-03-21T00:00:00.000+0000' AS updt_dt, 
  'LEG' AS src_sys_cd, 
  'Entity004' AS hfm_entity
UNION ALL
-- NULL handling scenario for sellable_qty
SELECT 
  '5' AS prod_id, 
  '126' AS item_nbr, 
  50.0 AS unit_cost, 
  20260201 AS prod_exp_dt,  -- valid prod_exp_dt in yyyymmdd format
  50.0 AS cost_per_pkg, 
  '202 Willow St' AS plant_add, 
  'US005' AS plant_loc_cd, 
  'PL500' AS prod_line, 
  'E-commerce' AS stock_type, 
  15.0 AS pre_prod_days, 
  NULL AS sellable_qty,  -- NULL sellable_qty
  'ORD567' AS prod_ordr_tracker_nbr, 
  '900' AS max_order_qty, 
  'Y' AS flag_active, 
  '2024-03-21T00:00:00.000+0000' AS crt_dt, 
  '2024-03-21T00:00:00.000+0000' AS updt_dt, 
  'CRM' AS src_sys_cd, 
  'Entity005' AS hfm_entity;

-- Insert generated test data into d_product table
INSERT INTO purgo_playground.d_product
SELECT * FROM temp_d_product_test_data;

-- SQL logic to validate data quality checks
-- Check for non-null item_nbr and sellable_qty

-- Count and display records with null item_nbr
SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

-- Display sample records with null item_nbr
SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

-- Count and display records with null sellable_qty
SELECT COUNT(*) AS null_sellable_qty_count
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

-- Display sample records with null sellable_qty
SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

-- Count and display records where prod_exp_dt is not in yyyymmdd format
SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM purgo_playground.d_product
WHERE LENGTH(CAST(prod_exp_dt AS STRING)) != 8 OR prod_exp_dt < 10000101 OR prod_exp_dt > 99991231;

-- Display sample records with invalid prod_exp_dt
SELECT *
FROM purgo_playground.d_product
WHERE LENGTH(CAST(prod_exp_dt AS STRING)) != 8 OR prod_exp_dt < 10000101 OR prod_exp_dt > 99991231
LIMIT 5;
