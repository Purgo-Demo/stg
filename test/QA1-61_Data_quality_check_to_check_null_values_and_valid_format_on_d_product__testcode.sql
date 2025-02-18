/* ---------------------------------------------------------------------------
   Test Code for Data Quality Validation in the Databricks Environment
   SQL-based tests for d_product table in purgo_playground schema
   ---------------------------------------------------------------------------
*/

/* ----------------------------------------------------------------------------
   Table Schema: purgo_playground.d_product 
   Columns:
   - prod_id, string
   - item_nbr, string
   - unit_cost, double
   - prod_exp_dt, decimal(38,0)
   - cost_per_pkg, double
   - plant_add, string
   - plant_loc_cd, string
   - prod_line, string
   - stock_type, string
   - pre_prod_days, double
   - sellable_qty, double
   - prod_ordr_tracker_nbr, string
   - max_order_qty, string
   - flag_active, string
   - crt_dt, timestamp
   - updt_dt, timestamp
   - src_sys_cd, string
   - hfm_entity, string
   ----------------------------------------------------------------------------
*/

/* ----------------------------------------------------------------------------
   Check for non-null item_nbr and sellable_qty in d_product table
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
   considering prod_exp_dt should be of numeric length 8 in the valid range
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
