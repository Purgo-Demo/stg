-- SQL Test Code for Data Quality Check on 'd_product' Table 

/* Setup Section: Ensure necessary configurations and environment setup */
-- Ensure necessary libraries and configurations are set up here if not available by default

/* Data Quality Test for 'd_product' Table */
-- Test to validate non-null 'item_nbr' and 'sellable_qty' in 'd_product' table
-- Test Schema: purgo_playground.d_product
-- Primary Key: prod_id

/* 
  Test Goal: 
  Validate that 'item_nbr' and 'sellable_qty' are not null
  Check for date format 'yyyymmdd' in 'prod_exp_dt'
  Display sample records for analysis
*/

/* Testing Assumptions: 
   - The 'prod_exp_dt' must match the pattern without separators, expected format 'YYYYMMDD'.
*/

/* Test for Null item_nbr */
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

/* Execute and Assert Null item_nbr Test Results */
SELECT * FROM NullItemNbr;
SELECT * FROM NullItemNbrSample;

/* Test for Null sellable_qty */
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

/* Execute and Assert Null sellable_qty Test Results */
SELECT * FROM NullSellableQty;
SELECT * FROM NullSellableQtySample;

/* Test for Invalid prod_exp_dt format */
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

/* Execute and Assert prod_exp_dt Format Test Results */
SELECT * FROM InvalidProdExpDtFormat;
SELECT * FROM InvalidProdExpDtFormatSample;

-- Clean-up section: Add any necessary cleanup technology if needed
