-- Generate test data for d_product table
CREATE OR REPLACE TEMP VIEW test_data AS
SELECT 'P001' AS prod_id, '1234' AS item_nbr, 50.0 AS unit_cost, CAST(20230301 AS DECIMAL(38,0)) AS prod_exp_dt, 
       10.0 AS cost_per_pkg, 'Address1' AS plant_add, 'LOC1' AS plant_loc_cd, 'Line1' AS prod_line, 
       'Type1' AS stock_type, 5.0 AS pre_prod_days, 100.0 AS sellable_qty, 'TRACK123' AS prod_ordr_tracker_nbr, 
       '500' AS max_order_qty, 'Y' AS flag_active, TIMESTAMP('2024-01-01T00:00:00.000+0000') AS crt_dt, 
       TIMESTAMP('2024-01-01T00:00:00.000+0000') AS updt_dt, 'SYS1' AS src_sys_cd, 'HFM1' AS hfm_entity
UNION ALL
SELECT 'P002', '5678', 55.0, CAST(20230302 AS DECIMAL(38,0)), 15.0, 'Address2', 'LOC2', 'Line2', 
       'Type2', 6.0, 200.0, 'TRACK124', '600', 'N', TIMESTAMP('2024-01-02T00:00:00.000+0000'), 
       TIMESTAMP('2024-01-02T00:00:00.000+0000'), 'SYS2', 'HFM2'
UNION ALL
-- Edge case with NULL values
SELECT 'P003', NULL, 60.0, CAST(20230303 AS DECIMAL(38,0)), 20.0, 'Address3', 'LOC3', 'Line3', 
       'Type3', 7.0, NULL, 'TRACK125', '700', 'Y', NULL, NULL, 'SYS3', 'HFM3'
UNION ALL
-- Special characters and invalid prod_exp_dt
SELECT 'P004', 'ITEM@#$', 65.0, CAST(99991231 AS DECIMAL(38,0)), 25.0, 'Addr$ess4', 'LOC4', 'Line4', 
       'Type$', 8.0, 300.0, 'TRACK126', '800', 'N', TIMESTAMP('2024-01-04T00:00:00.000+0000'), 
       TIMESTAMP('2024-02-04T00:00:00.000+0000'), 'SYS4', 'HFM4'
UNION ALL
-- Error case with invalid unit_cost
SELECT 'P005', '9012', -10.0, CAST(20230305 AS DECIMAL(38,0)), 30.0, 'Address5', 'LOC5', 'Line5', 
       'Type5', 9.0, 400.0, 'TRACK127', '900', 'Y', TIMESTAMP('2024-01-05T00:00:00.000+0000'), 
       TIMESTAMP('2024-03-05T00:00:00.000+0000'), 'SYS5', 'HFM5'
UNION ALL
-- Happy path
SELECT 'P006', '3456', 75.0, CAST(20230306 AS DECIMAL(38,0)), 35.0, 'Address6', 'LOC6', 'Line6', 
       'Type6', 10.0, 500.0, 'TRACK128', '1000', 'Y', TIMESTAMP('2024-01-06T00:00:00.000+0000'), 
       TIMESTAMP('2024-03-06T00:00:00.000+0000'), 'SYS6', 'HFM6';

-- SQL logic to check data quality in d_product table
-- Check for NULL 'item_nbr'
SELECT COUNT(*) AS count_null_item_nbr FROM purgo_playground.d_product WHERE item_nbr IS NULL;
SELECT * FROM purgo_playground.d_product WHERE item_nbr IS NULL LIMIT 5;

-- Check for NULL 'sellable_qty'
SELECT COUNT(*) AS count_null_sellable_qty FROM purgo_playground.d_product WHERE sellable_qty IS NULL;
SELECT * FROM purgo_playground.d_product WHERE sellable_qty IS NULL LIMIT 5;

-- Check 'prod_exp_dt' format
SELECT COUNT(*) AS count_invalid_prod_exp_dt FROM purgo_playground.d_product 
WHERE prod_exp_dt NOT BETWEEN 19000101 AND 20991231;
SELECT * FROM purgo_playground.d_product WHERE prod_exp_dt NOT BETWEEN 19000101 AND 20991231 LIMIT 5;
