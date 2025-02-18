-- Generate comprehensive test data for the purgo_playground.f_inv_movmnt table using Databricks Spark SQL syntax

-- Drop existing test data if necessary
DROP TABLE IF EXISTS purgo_playground.test_f_inv_movmnt;

-- Create a new test data table
CREATE TABLE purgo_playground.test_f_inv_movmnt (
  txn_id STRING,
  inv_loc STRING,
  financial_qty DOUBLE,
  net_qty DOUBLE,
  expired_qt DECIMAL(38,0),
  item_nbr STRING,
  unit_cost DOUBLE,
  um_rate DOUBLE,
  plant_loc_cd STRING,
  inv_stock_reference STRING,
  stock_type STRING,
  qty_on_hand DOUBLE,
  qty_shipped DOUBLE,
  cancel_dt DECIMAL(38,0),
  flag_active STRING,
  crt_dt TIMESTAMP,
  updt_dt TIMESTAMP
);

-- Insert 20-30 diverse test records covering various test scenarios
INSERT INTO purgo_playground.test_f_inv_movmnt VALUES
-- Happy path test data
('txn001', 'locA', 1000.0, 900.0, 0, 'item001', 10.5, 1.0, 'loc001', 'ref001', 'typeA', 100.0, 10.0, null, 'Y', '2024-03-21T00:00:00.000+0000', '2024-03-21T01:00:00.000+0000'),
('txn002', 'locB', 2000.0, 1800.0, 0, 'item002', 8.5, 1.5, 'loc002', 'ref002', 'typeB', 200.0, 20.0, null, 'N', '2024-03-22T00:00:00.000+0000', '2024-03-22T01:00:00.000+0000'),

-- Edge cases
('txn003', 'locC', 0.0, 0.0, 0, 'item003', 0.0, 0.0, 'loc003', 'ref003', 'typeC', 0.0, 0.0, null, 'Y', '2024-03-23T00:00:00.000+0000', '2024-03-23T01:00:00.000+0000'),
('txn004', 'locD', 1e12, 1e12, 0, 'item004', 1e12, 1.0, 'loc004', 'ref004', 'typeD', 1e12, 1e12, null, 'Y', '2024-03-24T00:00:00.000+0000', '2024-03-24T01:00:00.000+0000'),

-- NULL handling scenarios
('txn005', null, null, null, null, 'item005', null, null, null, null, null, null, null, null, null, null, null),

-- Special and multi-byte characters
('txn006', 'locáteß', 3000.0, 2500.0, 0, 'item006', 9.0, 1.2, 'loc766', 'reƒ006', '呉typeE', 300.0, 30.0, null, 'Y', '2024-03-25T00:00:00.000+0000', '2024-03-25T01:00:00.000+0000'),

-- Error case examples
-- Out-of-range values
('txn007', 'locE', -1000.0, -800.0, 0, 'item007', -5.5, 1.0, 'loc007', 'ref007', 'typeF', -100.0, -10.0, null, 'Y', '2024-03-26T00:00:00.000+0000', '2024-03-26T01:00:00.000+0000'),
-- Invalid combination scenarios
('txn008', 'locF', 1000.0, 0.0, 1000, 'item008', 10.5, 1.0, 'loc001', 'ref008', 'typeG', 0.0, 0.0, null, 'Y', '2024-03-27T00:00:00.000+0000', '2024-03-27T01:00:00.000+0000');
  
-- Example SQL for testing calculations based on the generated test data
-- Calculate total and risk inventory, then compute the percentage of inventory at risk
WITH ActiveInventory AS (
  SELECT 
    SUM(financial_qty) AS total_inventory,
    SUM(CASE WHEN flag_active = 'Y' THEN financial_qty ELSE 0 END) AS inventory_at_risk
  FROM purgo_playground.test_f_inv_movmnt
  WHERE dnsa_flag = 'yes'
)
SELECT 
  total_inventory,
  inventory_at_risk,
  (inventory_at_risk / total_inventory) * 100 AS inventory_at_risk_percentage
FROM ActiveInventory
WHERE total_inventory > 0;
