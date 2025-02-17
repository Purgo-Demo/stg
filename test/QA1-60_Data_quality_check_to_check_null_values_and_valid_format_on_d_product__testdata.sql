-- SQL to check null values and incorrect format in d_product table

-- Check and count records with null item_nbr
SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

-- Display 5 sample records with null item_nbr
SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

-- Check and count records with null sellable_qty
SELECT COUNT(*) AS null_sellable_qty_count
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

-- Display 5 sample records with null sellable_qty
SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

-- Check and count records with incorrect prod_exp_dt format
SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM purgo_playground.d_product
WHERE prod_exp_dt IS NOT NULL AND CAST(prod_exp_dt AS STRING) NOT RLIKE '^[0-9]{8}$';

-- Display 5 sample records with incorrect prod_exp_dt format
SELECT *
FROM purgo_playground.d_product
WHERE prod_exp_dt IS NOT NULL AND CAST(prod_exp_dt AS STRING) NOT RLIKE '^[0-9]{8}$'
LIMIT 5;
