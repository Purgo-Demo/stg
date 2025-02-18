-- SQL Script to generate test data for purgo_playground.d_product_clone

-- Drop existing tables if present
DROP TABLE IF EXISTS purgo_playground.d_product_clone;
DROP VIEW IF EXISTS purgo_playground.d_product_vw_clone;

-- Create d_product_clone table with additional 'source_country' column
CREATE TABLE purgo_playground.d_product_clone (
    prod_id STRING,
    item_nbr STRING,
    unit_cost DOUBLE,
    prod_exp_dt DECIMAL(38,0),
    cost_per_pkg DOUBLE,
    plant_add STRING,
    plant_loc_cd STRING,
    prod_line STRING,
    stock_type STRING,
    pre_prod_days DOUBLE,
    sellable_qty DOUBLE,
    prod_ordr_tracker_nbr STRING,
    max_order_qty STRING,
    flag_active STRING,
    crt_dt TIMESTAMP,
    updt_dt TIMESTAMP,
    src_sys_cd STRING,
    hfm_entity STRING,
    source_country STRING DEFAULT 'NL'
);

-- Populate the d_product_clone table with diverse test data
INSERT INTO purgo_playground.d_product_clone
VALUES
-- Happy path test records
('P01', '0001', 23.45, 20240101, 12.99, '123 Elm St', 'LOC001', 'Line A', 'Type 1', 10.0, 100.0, 'TRK001', '50', 'Y', '2024-01-01T00:00:00.000+0000', '2024-01-02T00:00:00.000+0000', 'SYS1', 'ENTITY01', 'NL'),
('P02', '0002', 50.00, 20240102, 25.50, '456 Oak St', 'LOC002', 'Line B', 'Type 2', 5.0, 200.0, 'TRK002', '100', 'Y', '2024-01-05T00:00:00.000+0000', '2024-01-06T00:00:00.000+0000', 'SYS2', 'ENTITY02', 'NL'),
-- Edge case test records
('P03', '0003', 0.0, 20231231, 0.0, '789 Pine St', 'LOC003', 'Line C', 'Type 3', 0.0, 0.0, 'TRK003', '0', 'N', '2023-12-31T23:59:59.999+0000', '2024-01-01T23:59:59.999+0000', 'SYS3', 'ENTITY03', 'NL'),
-- Error case: Out-of-range expiration date and negative cost
('P04', '0004', -10.00, -1000, -5.50, '135 Maple St', 'LOC004', 'Line D', 'Type 4', -2.0, -50.0, 'TRK004', '-10', 'Y', '2024-12-31T00:00:00.000+0000', '2024-12-31T00:00:00.000+0000', 'SYS4', 'ENTITY04', 'NL'),
-- NULL handling scenarios
(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'NL'),
-- Records with special and multi-byte characters
('P05', 'テスト', 10000.99, 20250101, 1200.75, '123 Straße', 'LOC005', 'Línea E', 'Typ€ 5', 50.0, 5000.0, 'TRK005', '500', 'N', '2025-01-01T12:00:00.000+0000', '2025-01-02T12:00:00.000+0000', 'SYS5', 'ENTITY05', 'NL');

-- Create dependent view d_product_vw_clone
CREATE VIEW purgo_playground.d_product_vw_clone AS
SELECT prod_id, item_nbr, unit_cost, prod_exp_dt, cost_per_pkg, plant_add, plant_loc_cd, prod_line, stock_type, pre_prod_days, 
       sellable_qty, prod_ordr_tracker_nbr, max_order_qty, flag_active, crt_dt, updt_dt, hfm_entity, source_country
FROM purgo_playground.d_product_clone;

-- Validate that the 'source_country' column has the default 'NL' value in both table and view
SELECT * FROM purgo_playground.d_product_clone;
SELECT * FROM purgo_playground.d_product_vw_clone;

