-- Feature: Data Quality Check on d_product Table

-- Set Unity Catalog schema
USE purgo_playground;

-- Data Quality Test for NULL checks on 'item_nbr' column

-- Count records where 'item_nbr' is NULL
SELECT COUNT(*) AS count_null_item_nbr 
FROM d_product 
WHERE item_nbr IS NULL;

-- Display up to 5 sample records where 'item_nbr' is NULL
SELECT * 
FROM d_product 
WHERE item_nbr IS NULL 
LIMIT 5;

-- Data Quality Test for NULL checks on 'sellable_qty' column

-- Count records where 'sellable_qty' is NULL
SELECT COUNT(*) AS count_null_sellable_qty 
FROM d_product 
WHERE sellable_qty IS NULL;

-- Display up to 5 sample records where 'sellable_qty' is NULL
SELECT * 
FROM d_product 
WHERE sellable_qty IS NULL 
LIMIT 5;

-- Data Quality Test for checking format of 'prod_exp_dt'
-- Using REGEXP to check if 'prod_exp_dt' is in YYYYMMDD format
-- Assuming that prod_exp_dt is stored as a string for date validation

-- Count records where 'prod_exp_dt' does not conform to YYYYMMDD format
SELECT COUNT(*) AS count_invalid_prod_exp_dt 
FROM d_product 
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$';

-- Display up to 5 sample records where 'prod_exp_dt' format is invalid
SELECT * 
FROM d_product 
WHERE NOT prod_exp_dt RLIKE '^[0-9]{8}$' 
LIMIT 5;

