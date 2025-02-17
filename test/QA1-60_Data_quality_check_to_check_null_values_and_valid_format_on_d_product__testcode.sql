-- SQL Test Code for Data Quality Checks on d_product Table

/* Section: Non-null Validation for item_nbr and sellable_qty */
/* Check for non-null 'item_nbr' and 'sellable_qty' in the d_product table */

-- Count records with null 'item_nbr'
SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

-- Display 5 sample records with null 'item_nbr'
SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

-- Count records with null 'sellable_qty'
SELECT COUNT(*) AS null_sellable_qty_count
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

-- Display 5 sample records with null 'sellable_qty'
SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

/* Section: Date Format Validation for prod_exp_dt */
/* Check for incorrect 'prod_exp_dt' format (not yyyymmdd) */

-- Count records with incorrect 'prod_exp_dt' format
SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM purgo_playground.d_product
WHERE prod_exp_dt IS NOT NULL AND CAST(prod_exp_dt AS STRING) NOT RLIKE '^[0-9]{8}$';

-- Display 5 sample records with incorrect 'prod_exp_dt' format
SELECT *
FROM purgo_playground.d_product
WHERE prod_exp_dt IS NOT NULL AND CAST(prod_exp_dt AS STRING) NOT RLIKE '^[0-9]{8}$'
LIMIT 5;

/* Section: Execution and Reporting */
/* Example of inserting the check result into report table */
INSERT INTO purgo_playground.dq_reports (Order_ID, Mandatory_Fields_Check, Date_Consistency_Check)
SELECT 1, 
       CASE WHEN (SELECT COUNT(*) FROM purgo_playground.d_product WHERE item_nbr IS NULL) = 0 
            AND (SELECT COUNT(*) FROM purgo_playground.d_product WHERE sellable_qty IS NULL) = 0 
       THEN 'No Nulls Found' ELSE 'Nulls Detected' END,
       CASE WHEN (SELECT COUNT(*) FROM purgo_playground.d_product WHERE prod_exp_dt IS NOT NULL AND CAST(prod_exp_dt AS STRING) NOT RLIKE '^[0-9]{8}$') = 0 
       THEN 'All Dates Valid' ELSE 'Invalid Dates Detected' END;

