-- Happy Path Test Data for the 'd_product' table
CREATE TABLE purgo_playground.d_product_happy_path AS
SELECT 
  CAST('PROD12345' AS STRING) AS prod_id,
  CAST('ITEM123' AS STRING) AS item_nbr,
  CAST(12.5 AS DOUBLE) AS unit_cost,
  CAST(20240321 AS DECIMAL(38,0)) AS prod_exp_dt,
  CAST(5.5 AS DOUBLE) AS cost_per_pkg,
  CAST('Plant A' AS STRING) AS plant_add,
  CAST('LOC1' AS STRING) AS plant_loc_cd,
  CAST('Line A' AS STRING) AS prod_line,
  CAST('STOCK' AS STRING) AS stock_type,
  CAST(10.0 AS DOUBLE) AS pre_prod_days,
  CAST(100.0 AS DOUBLE) AS sellable_qty,
  CAST('TRACK123' AS STRING) AS prod_ordr_tracker_nbr,
  CAST('500' AS STRING) AS max_order_qty,
  CAST('Y' AS STRING) AS flag_active,
  CAST('2024-03-21T00:00:00.000+0000' AS TIMESTAMP) AS crt_dt,
  CAST('2024-03-21T00:00:00.000+0000' AS TIMESTAMP) AS updt_dt,
  CAST('SYS01' AS STRING) AS src_sys_cd,
  CAST('ENTITY1' AS STRING) AS hfm_entity;

-- Edge Case Test Data for the 'd_product' table
INSERT INTO purgo_playground.d_product_happy_path VALUES
(CAST('PROD00001' AS STRING), NULL, CAST(0.0 AS DOUBLE), CAST(0 AS DECIMAL(38,0)), CAST(0.0 AS DOUBLE),
CAST('Plant Z' AS STRING), CAST('LOCZ' AS STRING), CAST('Line Z' AS STRING), CAST('NON-STOCK' AS STRING),
CAST(-1.0 AS DOUBLE), CAST(0.0 AS DOUBLE), NULL, CAST('0' AS STRING), CAST('N' AS STRING),
CAST('2023-01-01T00:00:00.000+0000' AS TIMESTAMP), CAST('2023-01-01T00:00:00.000+0000' AS TIMESTAMP), 
CAST('SYS99' AS STRING), CAST('ENTITYZ' AS STRING));

-- Error Case and Special Characters
INSERT INTO purgo_playground.d_product_happy_path VALUES
(CAST('PROD_ERR' AS STRING), CAST('ITEM@#*' AS STRING), CAST(-50.0 AS DOUBLE), CAST(99999999 AS DECIMAL(38,0)), 
CAST(-5.5 AS DOUBLE), CAST('Plant ^$%' AS STRING), CAST('Lloç' AS STRING), CAST('Línea A' AS STRING), 
CAST('INV@LID' AS STRING), CAST(-999.0 AS DOUBLE), NULL, CAST('TRACK$$$' AS STRING), NULL, 
CAST('N' AS STRING), CAST('2023-01-01T00:00:00.000+0000' AS TIMESTAMP), CAST('2023-01-01T00:00:00.000+0000' AS TIMESTAMP), 
CAST('' AS STRING), CAST('ENTITY' AS STRING));

-- Test Data for invalid date format in 'prod_exp_dt'
INSERT INTO purgo_playground.d_product_happy_path VALUES
(CAST('PROD_INV_DATE' AS STRING), CAST('ITEMINV' AS STRING), CAST(10.0 AS DOUBLE), CAST(20240321 AS DECIMAL(38,0) + 1), 
CAST(5.5 AS DOUBLE), CAST('Plant A' AS STRING), CAST('LOC1' AS STRING), CAST('Line A' AS STRING), 
CAST('STOCK' AS STRING), CAST(10.0 AS DOUBLE), CAST(100.0 AS DOUBLE), CAST('TRACK123' AS STRING), 
CAST('500' AS STRING), CAST('Y' AS STRING), CAST('2024-03-21T00:00:00.000+0000' AS TIMESTAMP), 
CAST('2024-03-21T00:00:00.000+0000' AS TIMESTAMP), CAST('SYS01' AS STRING), CAST('ENTITY1' AS STRING));

-- NULL handling scenario
INSERT INTO purgo_playground.d_product_happy_path VALUES
(CAST('PROD_NULL' AS STRING), NULL, CAST(20.0 AS DOUBLE), NULL, CAST(10.0 AS DOUBLE), NULL, 
NULL, NULL, NULL, CAST(0.0 AS DOUBLE), NULL, NULL, NULL, NULL, 
CAST('2023-01-01T00:00:00.000+0000' AS TIMESTAMP), CAST('2023-01-01T00:00:00.000+0000' AS TIMESTAMP), NULL, NULL);

-- Special characters and multi-byte characters
INSERT INTO purgo_playground.d_product_happy_path VALUES
(CAST('PROD_Ñ' AS STRING), CAST('ITEMÑ' AS STRING), CAST(15.0 AS DOUBLE), CAST(20230101 AS DECIMAL(38,0)), CAST(8.8 AS DOUBLE), 
CAST('Plánt Æ' AS STRING), CAST('Löc' AS STRING), CAST('Lïne A' AS STRING), 
CAST('STÖCK' AS STRING), CAST(12.5 AS DOUBLE), CAST(150.0 AS DOUBLE), 
CAST('TRACKü' AS STRING), CAST('1000' AS STRING), CAST('Y' AS STRING), 
CAST('2020-01-01T00:00:00.000+0000' AS TIMESTAMP), CAST('2020-01-01T00:00:00.000+0000' AS TIMESTAMP), 
CAST('SYSÑ' AS STRING), CAST('ENTITYÆ' AS STRING));



-- Query to Check for Not-Null Constraints and Generate Reports
WITH NullItemNbr AS (
  SELECT COUNT(*) AS null_item_nbr_count
  FROM purgo_playground.d_product
  WHERE item_nbr IS NULL
),
NullItemNbrSample AS (
  SELECT *
  FROM purgo_playground.d_product
  WHERE item_nbr IS NULL
  LIMIT 5
),
NullSellableQty AS (
  SELECT COUNT(*) AS null_sellable_qty_count
  FROM purgo_playground.d_product
  WHERE sellable_qty IS NULL
),
NullSellableQtySample AS (
  SELECT *
  FROM purgo_playground.d_product
  WHERE sellable_qty IS NULL
  LIMIT 5
),
InvalidProdExpDtFormat AS (
  SELECT COUNT(*) AS invalid_prod_exp_dt_format_count
  FROM purgo_playground.d_product
  WHERE CAST(prod_exp_dt AS STRING) NOT RLIKE '^[0-9]{8}$'
),
InvalidProdExpDtFormatSample AS (
  SELECT *
  FROM purgo_playground.d_product
  WHERE CAST(prod_exp_dt AS STRING) NOT RLIKE '^[0-9]{8}$'
  LIMIT 5
)
SELECT * FROM NullItemNbr;
SELECT * FROM NullItemNbrSample;

SELECT * FROM NullSellableQty;
SELECT * FROM NullSellableQtySample;

SELECT * FROM InvalidProdExpDtFormat;
SELECT * FROM InvalidProdExpDtFormatSample;
