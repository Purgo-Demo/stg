-- Use Databricks SQL to create test data for the given tables

-- Generate test data for purgo_playground.bien_schemaf_inv_movmnt
CREATE OR REPLACE TEMP VIEW bien_schemaf_inv_movmnt AS
SELECT
  txn_id,
  inv_loc,
  financial_qty,
  net_qty,
  expired_qt,
  item_nbr,
  unit_cost,
  um_rate,
  plant_loc_cd,
  inv_stock_reference,
  stock_type,
  qty_on_hand,
  qty_shipped,
  cancel_dt,
  flag_active,
  crt_dt,
  updt_dt
FROM VALUES
  ('TXN001', 'LOC001', 100.0, 90.0, 0, 'ITEM001', 10.0, 1.0, 'PLANT001', 'REF001', 'ACTIVE', 80.0, 20.0, 0, 'Y', '2023-03-01T00:00:00.000+0000', '2023-03-05T00:00:00.000+0000'),
  ('TXN002', 'LOC002', 200.0, 180.0, 0, 'ITEM002', 20.0, 1.0, 'PLANT002', 'REF002', 'ACTIVE', 170.0, 30.0, 0, 'Y', '2023-03-02T00:00:00.000+0000', '2023-03-06T00:00:00.000+0000'),
  ('TXN003', 'LOC001', 150.0, 140.0, 0, 'ITEM001', 15.0, 1.0, 'PLANT001', 'REF003', 'ACTIVE', 60.0, 50.0, 0, 'N', '2023-03-03T00:00:00.000+0000', '2023-03-07T00:00:00.000+0000');

-- Generate test data for purgo_playground.bien_schemaf_sales
CREATE OR REPLACE TEMP VIEW bien_schemaf_sales AS
SELECT
  ref_txn_id,
  item_nbr,
  qty_sold,
  order_qty,
  sched_dt,
  expected_shipped_dt,
  actual_shipped_dt,
  flag_return,
  flag_cancel,
  cancel_dt,
  cancel_qty,
  crt_dt,
  updt_dt
FROM VALUES
  ('TXN001', 'ITEM001', 40.0, 50.0, 0, 0, 0, 'N', 'N', 0, 0.0, '2023-03-01T00:00:00.000+0000', '2023-03-05T00:00:00.000+0000'),
  ('TXN002', 'ITEM002', 30.0, 30.0, 0, 0, 0, 'N', 'N', 0, 0.0, '2023-03-02T00:00:00.000+0000', '2023-03-06T00:00:00.000+0000'),
  ('TXN003', 'ITEM003', -10.0, 20.0, 0, 0, 0, 'Y', 'Y', 0, 10.0, '2023-03-03T00:00:00.000+0000', '2023-03-07T00:00:00.000+0000');

-- Generate test data for purgo_playground.doh_metrics
CREATE OR REPLACE TEMP VIEW doh_metrics AS
SELECT
  item_nbr,
  avg_inventory,
  avg_daily_sales,
  doh
FROM VALUES
  ('ITEM001', 150.0, 5.0, 1095.0),
  ('ITEM002', 200.0, 10.0, 730.0),
  ('ITEM003', 100.0, 4.5, 810.0);

-- Generate test data for purgo_playground.doh_kpis
CREATE OR REPLACE TEMP VIEW doh_kpis AS
SELECT
  avg_doh,
  stddev_doh
FROM VALUES
  (878.3, 182.1);

-- Generate test data for purgo_playground.f_inv_movmnt
CREATE OR REPLACE TEMP VIEW f_inv_movmnt AS
SELECT
  txn_id,
  inv_loc,
  financial_qty,
  net_qty,
  expired_qt,
  item_nbr,
  unit_cost,
  um_rate,
  plant_loc_cd,
  inv_stock_reference,
  stock_type,
  qty_on_hand,
  qty_shipped,
  cancel_dt,
  flag_active,
  crt_dt,
  updt_dt
FROM VALUES
  ('TXN101', 'LOC101', 400.0, 350.0, 0, 'ITEM101', 40.0, 1.0, 'PLANT101', 'REF101', 'ACTIVE', 300.0, 100.0, 0, 'Y', '2023-03-01T00:00:00.000+0000', '2023-03-05T00:00:00.000+0000'),
  ('TXN102', 'LOC102', 250.0, 230.0, 0, 'ITEM102', 25.0, 1.0, 'PLANT102', 'REF102', 'ACTIVE', 210.0, 90.0, 0, 'Y', '2023-03-02T00:00:00.000+0000', '2023-03-06T00:00:00.000+0000'),
  ('TXN103', 'LOC101', 300.0, 280.0, 0, 'ITEM103', 30.0, 1.0, 'PLANT101', 'REF103', 'ACTIVE', 200.0, 150.0, 0, 'N', '2023-03-03T00:00:00.000+0000', '2023-03-07T00:00:00.000+0000');

-- Generate test data for purgo_playground.f_sales
CREATE OR REPLACE TEMP VIEW f_sales AS
SELECT
  ref_txn_id,
  item_nbr,
  qty_sold,
  order_qty,
  sched_dt,
  expected_shipped_dt,
  actual_shipped_dt,
  flag_return,
  flag_cancel,
  cancel_dt,
  cancel_qty,
  crt_dt,
  updt_dt
FROM VALUES
  ('TXN101', 'ITEM101', 70.0, 80.0, 0, 0, 0, 'N', 'N', 0, 0.0, '2023-03-01T00:00:00.000+0000', '2023-03-05T00:00:00.000+0000'),
  ('TXN102', 'ITEM102', 60.0, 70.0, 0, 0, 0, 'N', 'N', 0, 0.0, '2023-03-02T00:00:00.000+0000', '2023-03-06T00:00:00.000+0000'),
  ('TXN103', 'ITEM103', -5.0, 15.0, 0, 0, 0, 'Y', 'Y', 0, 5.0, '2023-03-03T00:00:00.000+0000', '2023-03-07T00:00:00.000+0000');

-- Generate test data for purgo_playground.quang_schemaf_inv_movmnt
CREATE OR REPLACE TEMP VIEW quang_schemaf_inv_movmnt AS
SELECT
  txn_id,
  inv_loc,
  financial_qty,
  net_qty,
  expired_qt,
  item_nbr,
  unit_cost,
  um_rate,
  plant_loc_cd,
  inv_stock_reference,
  stock_type,
  qty_on_hand,
  qty_shipped,
  cancel_dt,
  flag_active,
  crt_dt,
  updt_dt
FROM VALUES
  ('TXN201', 'LOC201', 500.0, 420.0, 0, 'ITEM201', 50.0, 1.0, 'PLANT201', 'REF201', 'ACTIVE', 380.0, 120.0, 0, 'Y', '2023-03-01T00:00:00.000+0000', '2023-03-05T00:00:00.000+0000'),
  ('TXN202', 'LOC202', 300.0, 270.0, 0, 'ITEM202', 35.0, 1.0, 'PLANT202', 'REF202', 'ACTIVE', 290.0, 80.0, 0, 'Y', '2023-03-02T00:00:00.000+0000', '2023-03-06T00:00:00.000+0000'),
  ('TXN203', 'LOC201', 400.0, 380.0, 0, 'ITEM203', 45.0, 1.0, 'PLANT201', 'REF203', 'ACTIVE', 240.0, 160.0, 0, 'N', '2023-03-03T00:00:00.000+0000', '2023-03-07T00:00:00.000+0000');

-- Generate test data for purgo_playground.quang_schemaf_sales
CREATE OR REPLACE TEMP VIEW quang_schemaf_sales AS
SELECT
  ref_txn_id,
  item_nbr,
  qty_sold,
  order_qty,
  sched_dt,
  expected_shipped_dt,
  actual_shipped_dt,
  flag_return,
  flag_cancel,
  cancel_dt,
  cancel_qty,
  crt_dt,
  updt_dt
FROM VALUES
  ('TXN201', 'ITEM201', 65.0, 75.0, 0, 0, 0, 'N', 'N', 0, 0.0, '2023-03-01T00:00:00.000+0000', '2023-03-05T00:00:00.000+0000'),
  ('TXN202', 'ITEM202', 45.0, 55.0, 0, 0, 0, 'N', 'N', 0, 0.0, '2023-03-02T00:00:00.000+0000', '2023-03-06T00:00:00.000+0000'),
  ('TXN203', 'ITEM203', null, 25.0, 0, 0, 0, 'Y', 'Y', 0, 5.0, '2023-03-03T00:00:00.000+0000', '2023-03-07T00:00:00.000+0000');
