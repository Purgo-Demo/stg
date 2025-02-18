-- Step 1: Drop existing clone tables and views if they exist
DROP TABLE IF EXISTS purgo_playground.d_product_clone;
DROP VIEW IF EXISTS purgo_playground.d_product_vw_clone;

-- Step 2: Create a clone of the d_product table with an additional column 'source_country' 
CREATE TABLE purgo_playground.d_product_clone AS
SELECT *, 'NL' AS source_country
FROM purgo_playground.d_product;

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

-- Happy Path Test Data
-- Generate valid test data
INSERT INTO purgo_playground.d_product_clone (prod_id, item_nbr, unit_cost, prod_exp_dt, cost_per_pkg, plant_add, plant_loc_cd, prod_line, stock_type, pre_prod_days, sellable_qty, prod_ordr_tracker_nbr, max_order_qty, flag_active, crt_dt, updt_dt, src_sys_cd, hfm_entity, source_country)
VALUES 
('P01', 'I01', 100.50, 20250321, 12.5, 'Address1', 'LOC01', 'Line1', 'Stock1', 3.5, 50.0, 'TRACK1001', '200', 'Yes', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 'SRC001', 'Entity01', 'NL'),
('P02', 'I02', 200.75, 20240321, 15.0, 'Address2', 'LOC02', 'Line2', 'Stock2', 4.0, 60.0, 'TRACK1002', '300', 'No', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 'SRC002', 'Entity02', 'NL');

-- Edge Case Test Data
-- Generate boundary condition test data (e.g. zero-values)
INSERT INTO purgo_playground.d_product_clone (prod_id, item_nbr, unit_cost, prod_exp_dt, cost_per_pkg, plant_add, plant_loc_cd, prod_line, stock_type, pre_prod_days, sellable_qty, prod_ordr_tracker_nbr, max_order_qty, flag_active, crt_dt, updt_dt, src_sys_cd, hfm_entity, source_country)
VALUES 
('P03', 'I03', 0.0, 0, 0.0, '', '', '', '', 0.0, 0.0, '', '', 'Yes', '1970-01-01T00:00:00.000+0000', '1970-01-01T00:00:00.000+0000', '', '', 'NL');

-- Error Case Test Data
-- Generate data with invalid combinations (e.g., negative prices)
INSERT INTO purgo_playground.d_product_clone (prod_id, item_nbr, unit_cost, prod_exp_dt, cost_per_pkg, plant_add, plant_loc_cd, prod_line, stock_type, pre_prod_days, sellable_qty, prod_ordr_tracker_nbr, max_order_qty, flag_active, crt_dt, updt_dt, src_sys_cd, hfm_entity, source_country)
VALUES 
('P04', 'I04', -100.0, 20250101, -10.0, 'Address4', 'LOC04', 'Line4', 'Stock4', -1.0, -10.0, 'TRACK1004', '-100', 'No', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 'SRC004', 'Entity04', 'NL');

-- NULL Handling Test Data
-- Generate test data handling NULL
INSERT INTO purgo_playground.d_product_clone (prod_id, item_nbr, unit_cost, prod_exp_dt, cost_per_pkg, plant_add, plant_loc_cd, prod_line, stock_type, pre_prod_days, sellable_qty, prod_ordr_tracker_nbr, max_order_qty, flag_active, crt_dt, updt_dt, src_sys_cd, hfm_entity, source_country)
VALUES 
('P05', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'NL');

-- Special Characters Test Data
-- Generate test data with special/multi-byte characters
INSERT INTO purgo_playground.d_product_clone (prod_id, item_nbr, unit_cost, prod_exp_dt, cost_per_pkg, plant_add, plant_loc_cd, prod_line, stock_type, pre_prod_days, sellable_qty, prod_ordr_tracker_nbr, max_order_qty, flag_active, crt_dt, updt_dt, src_sys_cd, hfm_entity, source_country)
VALUES 
('P06', 'I06', 150.0, 20241111, 20.0, 'Āddressƒ', 'LØC06', 'Linè6', 'Stöck6', 5.0, 100.0, 'TRACK1006', '500', 'Yes', '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', 'SRC006', 'Entity06', 'NL');

-- Ensure the changes are reflected and test data is inserted into the view `d_product_vw_clone`
-- Validate test cases by querying the clone view
SELECT * FROM purgo_playground.d_product_vw_clone;

