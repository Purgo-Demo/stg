/* Test SQL for Inventory at Risk Calculation based on DNSA Events */

-- Setup: Create Test Temporary Views to simulate data scenarios

-- Create and populate test_f_inv_movmnt view
CREATE OR REPLACE TEMP VIEW test_f_inv_movmnt AS
SELECT * FROM VALUES
  ("txn_001", "loc_001", 100.0, 95.0, 0, "item_001", 10.0, 1.0, "loc_cd_001", "ref_001", "type_001", 500.0, 5.0, 20210321, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
  ("txn_002", "loc_002", 200.0, 190.0, 0, "item_002", 20.0, 1.0, "loc_cd_002", "ref_002", "type_002", 700.0, 10.0, 20220321, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
  ("txn_003", "loc_003", 0.0, -5.0, 0, "item_003", 0.01, 1.0, "loc_cd_003", "ref_003", "type_003", 50.0, 0.0, 20230321, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
  ("txn_004", "loc_004", 999999.99, 999999.99, 0, "item_004", 9999.99, 1.0, "loc_cd_004", "ref_004", "type_004", 50000.0, 0.0, 20240121, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
  ("txn_005", "loc_005", -10.0, 0.0, 0, "item_005", -5.0, 1.0, "loc_cd_005", "ref_005", "type_005", -500.0, -5.0, 20240122, "N", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
  (NULL, "loc_006", NULL, 100.0, NULL, "item_006", NULL, NULL, "loc_cd_006", NULL, "type_006", NULL, NULL, NULL, NULL, NULL, NULL),
  ("txn_007", "loc_漢字", 500.0, 480.0, 0, "アイテム_007", 50.0, 1.0, "loc_cd_汉字", "ref_漢字", "type_漢字", 200.0, 20.0, 20231010, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000")
AS t(txn_id, inv_loc, financial_qty, net_qty, expired_qt, item_nbr, unit_cost, um_rate, plant_loc_cd, inv_stock_reference, stock_type, qty_on_hand, qty_shipped, cancel_dt, flag_active, crt_dt, updt_dt);

-- Mock view for total inventory calculation, assuming aggregates from related tables
CREATE OR REPLACE TEMP VIEW test_total_inventory AS
SELECT SUM(financial_qty) AS total_inventory
FROM test_f_inv_movmnt;

-- Inventory at Risk Calculation
SELECT 
  SUM(CASE WHEN flag_active = 'Y' THEN financial_qty ELSE 0 END) AS inventory_at_risk
FROM test_f_inv_movmnt;

-- Percentage of Inventory at Risk Calculation
SELECT 
  (inventory_at_risk_calculation.inventory_at_risk / total_inventory.total_inventory) * 100 AS percentage_of_inventory_at_risk
FROM (
  SELECT 
    SUM(CASE WHEN flag_active = 'Y' THEN financial_qty ELSE 0 END) AS inventory_at_risk
  FROM test_f_inv_movmnt
) inventory_at_risk_calculation,
test_total_inventory total_inventory;

-- Perform NULL handling tests
-- Show records with potential NULL values in critical fields
SELECT
  *
FROM
  test_f_inv_movmnt
WHERE
  financial_qty IS NULL OR flag_active IS NULL;

-- Clean up test views
-- Note: Since we are using temporary views, they will automatically clean up at the end of the session
