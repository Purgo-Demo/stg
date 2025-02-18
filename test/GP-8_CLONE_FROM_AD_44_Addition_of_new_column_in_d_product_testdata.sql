-- Test Data Generation: Databricks SQL for `d_product_clone` table

-- Drop the existing clone table if it exists
DROP TABLE IF EXISTS purgo_playground.d_product_clone;

-- Create a replica of the d_product table with the new column 'source_country' hardcoded to 'NL'
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
  'NL' AS source_country -- Hardcoded value for source_country
FROM purgo_playground.d_product;

-- Ensure all records have 'NL' for source_country
SELECT * FROM purgo_playground.d_product_clone;

-- Test data for various scenarios:
-- Happy Path: Valid Data
INSERT INTO purgo_playground.d_product_clone (prod_id, item_nbr, unit_cost, prod_exp_dt, cost_per_pkg, plant_add, plant_loc_cd, prod_line, stock_type, pre_prod_days, sellable_qty, prod_ordr_tracker_nbr, max_order_qty, flag_active, crt_dt, updt_dt, src_sys_cd, hfm_entity, source_country)
VALUES
  ('P001', 'ITEM001', 10.5, 20240425, 15.75, 'Main Plant', 'LOC1', 'LINE1', 'STOCK1', 2.5, 150.0, 'TRK123', '500', 'Y', '2024-03-21T00:00:00.000+0000', '2024-03-22T00:00:00.000+0000', 'SYS1', 'ENTITY1', 'NL'),
  ('P002', 'ITEM002', 5.0, 20240530, 12.25, 'Secondary Plant', 'LOC2', 'LINE2', 'STOCK2', 1.0, 300.0, 'TRK124', '300', 'N', '2024-03-22T00:00:00.000+0000', '2024-03-23T00:00:00.000+0000', 'SYS2', 'ENTITY2', 'NL');

-- Edge Cases: Boundary Testing
-- Pre-production days at 0
INSERT INTO purgo_playground.d_product_clone (prod_id, item_nbr, unit_cost, prod_exp_dt, cost_per_pkg, plant_add, plant_loc_cd, prod_line, stock_type, pre_prod_days, sellable_qty, prod_ordr_tracker_nbr, max_order_qty, flag_active, crt_dt, updt_dt, src_sys_cd, hfm_entity, source_country)
VALUES
  ('P003', 'ITEM003', 11.5, 20241231, 18.75, 'Main Plant', 'LOC1', 'LINE1', 'STOCK1', 0.0, 200.0, 'TRK125', '600', 'Y', '2024-01-01T00:00:00.000+0000', '2024-02-01T00:00:00.000+0000', 'SYS3', 'ENTITY3', 'NL');
  
-- Sellable quantity at max threshold
INSERT INTO purgo_playground.d_product_clone (prod_id, item_nbr, unit_cost, prod_exp_dt, cost_per_pkg, plant_add, plant_loc_cd, prod_line, stock_type, pre_prod_days, sellable_qty, prod_ordr_tracker_nbr, max_order_qty, flag_active, crt_dt, updt_dt, src_sys_cd, hfm_entity, source_country)
VALUES
  ('P004', 'ITEM004', 9.75, 20240601, 20.0, 'Secondary Plant', 'LOC2', 'LINE2', 'STOCK2', 3.0, 9999.9, 'TRK126', '1000', 'Y', '2024-03-15T00:00:00.000+0000', '2024-03-20T00:00:00.000+0000', 'SYS4', 'ENTITY4', 'NL');

-- Error Cases: Invalid Data
-- Negative cost
INSERT INTO purgo_playground.d_product_clone (prod_id, item_nbr, unit_cost, prod_exp_dt, cost_per_pkg, plant_add, plant_loc_cd, prod_line, stock_type, pre_prod_days, sellable_qty, prod_ordr_tracker_nbr, max_order_qty, flag_active, crt_dt, updt_dt, src_sys_cd, hfm_entity, source_country)
VALUES
  ('P005', 'ITEM005', -1.0, 20240715, 19.5, 'Unknown Plant', 'LOC3', 'LINE3', 'STOCK3', 5.0, 0.0, 'TRK127', '10', 'N', '2024-04-01T00:00:00.000+0000', '2024-04-02T00:00:00.000+0000', 'SYS5', 'ENTITY5', 'NL');

-- NULL Handling Scenarios
INSERT INTO purgo_playground.d_product_clone (prod_id, item_nbr, unit_cost, prod_exp_dt, cost_per_pkg, plant_add, plant_loc_cd, prod_line, stock_type, pre_prod_days, sellable_qty, prod_ordr_tracker_nbr, max_order_qty, flag_active, crt_dt, updt_dt, src_sys_cd, hfm_entity, source_country)
VALUES
  ('P006', 'ITEM006', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'NL');

-- Special Characters and Multi-byte Characters
INSERT INTO purgo_playground.d_product_clone (prod_id, item_nbr, unit_cost, prod_exp_dt, cost_per_pkg, plant_add, plant_loc_cd, prod_line, stock_type, pre_prod_days, sellable_qty, prod_ordr_tracker_nbr, max_order_qty, flag_active, crt_dt, updt_dt, src_sys_cd, hfm_entity, source_country)
VALUES
  ('P007', 'ITEM007', 20.5, 20241015, 21.5, 'Main Plant €¥', 'LOC4®', 'LINE4©', 'STOCK4', 0.2, 100.0, 'TRK128', '999', 'N', '2024-05-01T00:00:00.000+0000', '2024-05-05T00:00:00.000+0000', 'SYS6', 'ENTITY6', 'NL');

-- Display the inserted test records from the clone table
SELECT * FROM purgo_playground.d_product_clone;

