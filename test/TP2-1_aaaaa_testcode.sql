-- Test code for validating Days on Hand (DOH) usage calculation in Databricks SQL environment

/*-------------------------------------
    Framework and Structure Setup
-------------------------------------*/

-- Using Databricks SQL syntax to create temporary views for testing
USE test_db;  -- Ensure testing within the scope of our test_db schema

-- Temporary view to simulate average inventory calculation
CREATE OR REPLACE TEMP VIEW average_inventory AS
SELECT 
    productId,
    plantId,
    AVG(inventoryLevel) AS average_inventory
FROM 
    f_inv_movmnt
GROUP BY 
    productId, plantId;

-- Temporary view to simulate average daily sales calculation
CREATE OR REPLACE TEMP VIEW average_daily_sales AS
SELECT 
    productId,
    plantId,
    AVG(salesAmount) AS average_daily_sales
FROM 
    f_sales
GROUP BY 
    productId, plantId;

/*-------------------------------------
    Data Type Testing
-------------------------------------*/

-- Validate conversion tests for numeric calculations
SELECT 
    CAST(average_inventory AS DOUBLE) AS inventory_check,
    CAST(average_daily_sales AS DOUBLE) AS sales_check,
    CASE WHEN average_inventory IS NULL THEN 'FAIL' ELSE 'PASS' END AS inventory_null_check,
    CASE WHEN average_daily_sales IS NULL THEN 'FAIL' ELSE 'PASS' END AS sales_null_check
FROM 
    average_inventory
JOIN 
    average_daily_sales USING (productId, plantId);

/*-------------------------------------
    Test Coverage
-------------------------------------*/

-- Unit test: Validate individual calculation of DOH
CREATE OR REPLACE TEMP VIEW doh_calculation AS
SELECT
    ai.productId,
    ai.plantId,
    (ai.average_inventory / ads.average_daily_sales) * 365 AS calculated_doh
FROM
    average_inventory ai
JOIN
    average_daily_sales ads
ON 
    ai.productId = ads.productId AND ai.plantId = ads.plantId;

-- Unit test assertion
SELECT 
    productId, plantId,
    CASE WHEN calculated_doh >= 0 THEN 'PASS' ELSE 'FAIL' END AS doh_positive_check
FROM 
    doh_calculation;

/*-------------------------------------
    Data Quality Validation Tests
-------------------------------------*/

-- Integration test: Validate full DOH KPI calculations
CREATE OR REPLACE TEMP VIEW doh_kpi_calculation AS
SELECT
    productId,
    plantId,
    AVG(calculated_doh) AS average_doh,
    STDDEV(calculated_doh) AS doh_std_dev
FROM
    doh_calculation
GROUP BY 
    productId, plantId;

-- Integration test assertion
SELECT 
    productId, plantId,
    CASE 
        WHEN average_doh IS NOT NULL AND doh_std_dev IS NOT NULL THEN 'PASS' 
        ELSE 'FAIL' 
    END AS doh_kpi_completeness_check
FROM 
    doh_kpi_calculation;

/*-------------------------------------
    Databricks-Specific Features 
-------------------------------------*/

-- Validate MERGE operation for Delta Lake functionality
MERGE INTO doh_calculation dc
USING (
    SELECT productId, plantId, calculated_doh 
    FROM doh_calculation 
    WHERE calculated_doh IS NOT NULL
) AS update_source
ON dc.productId = update_source.productId AND dc.plantId = update_source.plantId
WHEN MATCHED THEN 
    UPDATE SET calculated_doh = update_source.calculated_doh
WHEN NOT MATCHED THEN 
    INSERT (productId, plantId, calculated_doh) 
    VALUES (update_source.productId, update_source.plantId, update_source.calculated_doh);

-- Validate clean-up operations
DROP VIEW IF EXISTS average_inventory;
DROP VIEW IF EXISTS average_daily_sales;
DROP VIEW IF EXISTS doh_calculation;
DROP VIEW IF EXISTS doh_kpi_calculation;

-- End of test code
