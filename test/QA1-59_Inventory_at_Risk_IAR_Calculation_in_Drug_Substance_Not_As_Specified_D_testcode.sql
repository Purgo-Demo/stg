/* 
  Setup: Creating Temporary Views and Installing Required Libraries 
  Ensure necessary test dataset is available for testing Databricks SQL functionality
*/

-- Assuming libraries are installed as required in Databricks environment

-- Load test data for `f_inv_movmnt` into a temporary view
CREATE OR REPLACE TEMP VIEW test_f_inv_movmnt AS
SELECT * FROM VALUES
  ('txn001', 'loc001', 100.0, 150.0, 10, 'item001', 15.0, 1.0, 'PL01', 'ref001', 'ST01', 200.0, 50.0, 20231225, 'Y', TIMESTAMP('2023-04-21T00:00:00.000+0000'), TIMESTAMP('2023-04-21T12:00:00.000+0000')),
  ('txn002', 'loc002', 300.0, 350.0, 0, 'item002', 20.0, 1.5, 'PL02', 'ref002', 'ST02', 400.0, 100.0, 20240101, 'N', TIMESTAMP('2023-05-10T00:00:00.000+0000'), TIMESTAMP('2023-05-10T14:00:00.000+0000')),
  ('txn003', 'loc003', 0.0, 0.0, 0, 'item003', 0.0, 0.0, 'PL03', 'ref003', 'ST03', 0.0, 0.0, 0, 'N', TIMESTAMP('2024-02-28T00:00:00.000+0000'), TIMESTAMP('2024-02-28T15:00:00.000+0000')),
  ('txn004', 'loc004', 9999999999.99, 9999999999.99, 9999999999, 'item004', 9999999999.99, 9999999999.99, 'PL04', 'ref004', 'ST04', 9999999999.99, 9999999999.99, 9999999999, 'Y', TIMESTAMP('2024-03-21T00:00:00.000+0000'), TIMESTAMP('2024-03-21T16:00:00.000+0000')),
  ('txn005', 'loc005', -100.0, 200.0, -10, 'item005', -15.0, 1.0, 'PL05', 'ref005', 'ST01', -300.0, -50.0, 20231225, 'N', TIMESTAMP('2023-07-15T00:00:00.000+0000'), TIMESTAMP('2023-07-15T12:00:00.000+0000')),
  ('txn006', 'loc006', 200.0, 200.0, 0, 'item006', -20.0, -1.5, 'PL06', 'ref006', 'ST05', 200.0, 50.0, 20230101, 'Y', TIMESTAMP('2023-08-12T00:00:00.000+0000'), TIMESTAMP('2023-08-12T13:00:00.000+0000')),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  ('txn007', NULL, 100.0, NULL, NULL, 'item007', NULL, 1.0, NULL, 'ref007', NULL, 200.0, NULL, 20231225, 'Y', TIMESTAMP('2023-09-22T00:00:00.000+0000'), NULL),
  ('txn008', 'loc@#$%', 250.0, 200.0, 5, 'item◎☃', 25.0, 1.2, 'PL07', 'ref008', 'ST07', 300.0, 60.0, 20240125, 'Y', TIMESTAMP('2024-01-01T00:00:00.000+0000'), TIMESTAMP('2024-01-01T17:00:00.000+0000')),
  ('txn009', 'loc009', 150.0, 100.0, 0, 'item009✓✗', 10.0, 5.0, 'PL08', 'ref009☂', 'ST08', 100.0, 10.0, 20241231, 'N', TIMESTAMP('2024-06-30T00:00:00.000+0000'), TIMESTAMP('2024-06-30T18:00:00.000+0000'))
AS (txn_id, inv_loc, financial_qty, net_qty, expired_qt, item_nbr, unit_cost, um_rate, plant_loc_cd, inv_stock_reference, stock_type, qty_on_hand, qty_shipped, cancel_dt, flag_active, crt_dt, updt_dt);

-- Unit Test for Inventory at Risk Calculation
SELECT
  SUM(CASE WHEN flag_active = 'Y' THEN financial_qty ELSE 0 END) AS inventory_at_risk,
  SUM(financial_qty) AS total_inventory,
  (SUM(CASE WHEN flag_active = 'Y' THEN financial_qty ELSE 0 END) / SUM(financial_qty)) * 100 AS percentage_inventory_at_risk
FROM
  test_f_inv_movmnt;

-- Data Type and NULL Handling Test
SELECT
  INV_LOC AS location,
  CASE WHEN UNIT_COST IS NULL THEN "NULL" ELSE CAST(UNIT_COST AS STRING) END AS unit_cost_str
FROM
  test_f_inv_movmnt
WHERE
  ITEM_NBR IS NULL OR UNIT_COST IS NULL;

-- Integration Test: Assert correct join and calculation across tables
CREATE OR REPLACE TEMP VIEW test_d_product AS
SELECT * FROM VALUES
  ('prod001', 'item001', 15.0, 20231225, 1.5, 'Plant1', 'PL01', 'Line1', 'ST01', 5.0, 200.0, 'ord001', '100', 'Y', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'SYS', 'HFM')
AS (prod_id, item_nbr, unit_cost, prod_exp_dt, cost_per_pkg, plant_add, plant_loc_cd, prod_line, stock_type, pre_prod_days, sellable_qty, prod_ordr_tracker_nbr, max_order_qty, flag_active, crt_dt, updt_dt, src_sys_cd, hfm_entity);

SELECT
  f.txn_id,
  f.inv_loc,
  f.financial_qty,
  d.prod_id,
  d.unit_cost,
  d.stock_type
FROM
  test_f_inv_movmnt AS f
INNER JOIN
  test_d_product AS d
ON
  f.item_nbr = d.item_nbr
WHERE
  f.flag_active = 'Y';

-- Performance Test: Evaluate execution time for large dataset scenario
SELECT /*+ REPARTITION(100) */
  flag_active,
  COUNT(*) AS record_count
FROM
  test_f_inv_movmnt
GROUP BY
  flag_active;

-- Clean Up Test Artifacts
DROP VIEW IF EXISTS test_f_inv_movmnt;
DROP VIEW IF EXISTS test_d_product;
