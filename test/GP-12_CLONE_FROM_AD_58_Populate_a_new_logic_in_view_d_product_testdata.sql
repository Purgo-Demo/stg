-- Generate test data for the d_product table with a diverse range of scenarios

-- Drop existing table if it exists
DROP TABLE IF EXISTS purgo_playground.d_product_test_scenarios;

-- Create table with similar schema to d_product
CREATE TABLE purgo_playground.d_product_test_scenarios (
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
    hfm_entity STRING
);

-- Insert test data following different scenarios
INSERT INTO purgo_playground.d_product_test_scenarios
VALUES
-- Happy path scenarios
('P001', 'I001', 12.5, 1700000000000000000, 25.0, 'Address 1', 'LOC1', 'LineA', 'STK_TYPE_1', 5.0, 100.0, 'TRACK001', '200', 'Y', '2023-01-01T00:00:00.000+0000', '2023-10-01T00:00:00.000+0000', 'orafin', NULL),
('P002', 'I002', 15.0, 1700000000000000000, 30.0, 'Address 2', 'LOC2', 'LineB', 'STK_TYPE_2', 10.0, 150.0, 'TRACK002', '300', 'Y', '2023-01-05T00:00:00.000+0000', '2023-10-10T00:00:00.000+0000', 'orafin', NULL),

-- Edge cases
('P012', 'I010', 0.0, 1, 0.0, '', '', '', '', 0.0, 0.0, NULL, '', '', '2024-12-31T23:59:59.000+0000', '9999-12-31T23:59:59.000+0000', '', NULL), -- Edge of timestamp and empty structures

-- Error cases with valid data type but invalid business logic
('P101', 'I101', -5.0, 0, 0.0, 'Address X', 'LOCX', 'LineX', 'STK_TYPE_X', -1.0, -10.0, 'TRACK999', '-1', 'N', '2022-01-01T00:00:00.000+0000', '2022-01-01T00:00:00.000+0000', 'unknown_src', NULL), -- Negative values, invalid src_sys_cd

-- Special characters and multi-byte characters
('P131', 'I132', 23.0, 1700000000000000000, 45.0, 'Adresse Ü', 'LOÇ3', 'LíneC', 'STK_TYPE_3', 12.5, 230.0, 'TRACK789', '500', 'Y', '2023-01-03T12:00:00.000+0000', '2023-10-03T12:00:00.000+0000', 'orafin', NULL), -- Multi-byte chars

-- NULL handling
('P202', 'I202', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'null_sys', NULL); -- NULL values handling


-- SQL to populate a column hfm_entity based on src_sys_cd and join with edp_lkup
CREATE OR REPLACE VIEW purgo_playground.d_product_vw AS
SELECT
    dp.prod_id,
    dp.item_nbr,
    dp.unit_cost,
    dp.prod_exp_dt,
    dp.cost_per_pkg,
    dp.plant_add,
    dp.plant_loc_cd,
    dp.prod_line,
    dp.stock_type,
    dp.pre_prod_days,
    dp.sellable_qty,
    dp.prod_ordr_tracker_nbr,
    dp.max_order_qty,
    dp.flag_active,
    dp.crt_dt,
    dp.updt_dt,
    CASE WHEN dp.src_sys_cd = 'orafin' THEN el.lkkup_val_01 ELSE dp.hfm_entity END AS hfm_entity
FROM
    purgo_playground.d_product dp
LEFT JOIN
    purgo_playground.edp_lkup el
ON
    dp.stock_type = el.lkup_key_01;

