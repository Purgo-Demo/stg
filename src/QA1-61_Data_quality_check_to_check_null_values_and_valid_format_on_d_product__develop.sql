/* ---------------------------------------------------------------------------
   Data Quality Check for d_product Table
   Purpose: Validate not null and format constraints before moving to PROD.
   Enables correct data reporting by preventing erroneous data propagation.
   ---------------------------------------------------------------------------
*/

/* ----------------------------------------------------------------------------
   Check for non-null item_nbr in d_product table
   - Get the count of records where item_nbr is null
   - Display 5 sample records where item_nbr is null
   ----------------------------------------------------------------------------
*/

SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

/* ----------------------------------------------------------------------------
   Check for non-null sellable_qty in d_product table
   - Get the count of records where sellable_qty is null
   - Display 5 sample records where sellable_qty is null
   ----------------------------------------------------------------------------
*/

SELECT COUNT(*) AS null_sellable_qty_count
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

/* ----------------------------------------------------------------------------
   Validate prod_exp_dt format as yyyymmdd in d_product table
   - Get the count of records where prod_exp_dt does not conform to yyyymmdd
   - Display 5 sample records where prod_exp_dt does not conform to yyyymmdd
   Considering prod_exp_dt should be of numeric length 8 in the valid range
   ----------------------------------------------------------------------------
*/

SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM purgo_playground.d_product
WHERE LENGTH(CAST(prod_exp_dt AS STRING)) != 8
      OR prod_exp_dt < 10000101
      OR prod_exp_dt > 99991231;

SELECT *
FROM purgo_playground.d_product
WHERE LENGTH(CAST(prod_exp_dt AS STRING)) != 8
      OR prod_exp_dt < 10000101
      OR prod_exp_dt > 99991231
LIMIT 5;
