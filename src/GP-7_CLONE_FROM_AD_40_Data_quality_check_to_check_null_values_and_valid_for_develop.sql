-- SQL Code for Data Quality Check on 'd_product' Table

/* 
  Data Quality Check for Non-Null Values and Date Format
  Table: purgo_playground.d_product
  Primary Key: prod_id
*/

/* Checking for Null 'item_nbr' */
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
)

-- Retrieve Count and Sample Records for Null 'item_nbr'
SELECT * FROM NullItemNbr;
SELECT * FROM NullItemNbrSample;

/* Checking for Null 'sellable_qty' */
WITH NullSellableQty AS (
  SELECT COUNT(*) AS null_sellable_qty_count
  FROM purgo_playground.d_product
  WHERE sellable_qty IS NULL
),
NullSellableQtySample AS (
  SELECT *
  FROM purgo_playground.d_product
  WHERE sellable_qty IS NULL
  LIMIT 5
)

-- Retrieve Count and Sample Records for Null 'sellable_qty'
SELECT * FROM NullSellableQty;
SELECT * FROM NullSellableQtySample;

/* Checking for Incorrect 'prod_exp_dt' Format */
WITH InvalidProdExpDtFormat AS (
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

-- Retrieve Count and Sample Records for Incorrect 'prod_exp_dt' Format
SELECT * FROM InvalidProdExpDtFormat;
SELECT * FROM InvalidProdExpDtFormatSample;
