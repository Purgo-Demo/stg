-- Databricks SQL implementation code for Days on Hand (DOH) calculation

/*-------------------------------------
    Unity Catalog Schema Setup
-------------------------------------*/

USE catalog_name.schema_name;  -- Specify the Unity Catalog Schema

/*-------------------------------------
    Data Preparation and Setup
-------------------------------------*/

-- Temporary view for average inventory calculation
CREATE OR REPLACE TEMP VIEW average_inventory AS
SELECT 
    productId,
    plantId,
    AVG(inventoryLevel) AS average_inventory
FROM 
    f_inv_movmnt
WHERE 
    inventoryLevel IS NOT NULL  -- Handle NULLs in inventory data
GROUP BY 
    productId, plantId;

-- Temporary view for average daily sales calculation
CREATE OR REPLACE TEMP VIEW average_daily_sales AS
SELECT 
    productId,
    plantId,
    AVG(salesAmount) AS average_daily_sales
FROM 
    f_sales
WHERE 
    salesAmount IS NOT NULL  -- Handle NULLs in sales data
GROUP BY 
    productId, plantId;

/*-------------------------------------
    DOH Calculation
-------------------------------------*/

-- Calculate DOH using average inventory and average daily sales
CREATE OR REPLACE TEMP VIEW doh_calculation AS
SELECT
    ai.productId,
    ai.plantId,
    CASE 
        WHEN ads.average_daily_sales > 0 THEN (ai.average_inventory / ads.average_daily_sales) * 365 
        ELSE NULL  -- Handle division by zero
    END AS calculated_doh
FROM
    average_inventory ai
JOIN
    average_daily_sales ads
ON 
    ai.productId = ads.productId AND ai.plantId = ads.plantId;

/*-------------------------------------
    DOH KPI Calculation
-------------------------------------*/

-- Calculate DOH KPI values (average, stddev)
CREATE OR REPLACE TEMP VIEW doh_kpi AS
SELECT
    productId,
    plantId,
    AVG(calculated_doh) AS avg_doh,
    STDDEV(calculated_doh) AS std_doh
FROM
    doh_calculation
GROUP BY 
    productId, plantId;

/*-------------------------------------
    Performance Optimization
-------------------------------------*/

-- Suggestion for optimization using Z-Ordering
-- Run this command manually on a Delta Lake table instead of a temporary view
-- "OPTIMIZE target_doh_table ZORDER BY (productId, plantId);"

/*-------------------------------------
    Delta Lake Integration
-------------------------------------*/

-- Merge DOH data into a Delta table for consistent view and updates
MERGE INTO target_doh_table t
USING doh_calculation dc
ON t.productId = dc.productId AND t.plantId = dc.plantId
WHEN MATCHED THEN 
    UPDATE SET t.calculated_doh = dc.calculated_doh
WHEN NOT MATCHED THEN 
    INSERT (productId, plantId, calculated_doh)
    VALUES (dc.productId, dc.plantId, dc.calculated_doh);

/*-------------------------------------
    Data Cleanup and Validation
-------------------------------------*/

-- Validate that DOH values are within expected thresholds (example threshold logic)
SELECT 
    productId, plantId,
    CASE 
        WHEN avg_doh < 0 THEN 'FAIL: Negative DOH'
        WHEN avg_doh > 365 THEN 'FAIL: DOH exceeds 1 year'
        ELSE 'PASS'
    END AS validation_status
FROM 
    doh_kpi;

/*-------------------------------------
    Clean-Up
-------------------------------------*/

-- Drop temporary views to maintain a tidy workspace
DROP VIEW IF EXISTS average_inventory;
DROP VIEW IF EXISTS average_daily_sales;
DROP VIEW IF EXISTS doh_calculation;
DROP VIEW IF EXISTS doh_kpi;

/*-------------------------------------
    End of Implementation Code
-------------------------------------*/
