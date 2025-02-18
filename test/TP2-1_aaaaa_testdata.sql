-- Ensure required libraries are installed
-- In Databricks, you may install necessary libraries via the UI or use init scripts.
-- This segment of code assumes necessary libraries are pre-installed.

-- Create a temporary view for f_inv_movmnt with diverse test cases
CREATE OR REPLACE TEMPORARY VIEW f_inv_movmnt AS
SELECT 
    -- Happy path: valid inventory movements
    101 AS productId,
    '2024-01-15T00:00:00.000+0000' AS timestamp,
    100.0 AS inventoryQuantity,
    'A1' AS plantId
UNION ALL SELECT 
    -- Edge case: Zero inventory
    101, '2024-02-01T00:00:00.000+0000', 0.0, 'A1'
UNION ALL SELECT 
    -- Error case: Negative inventory
    101, '2024-02-15T00:00:00.000+0000', -5.0, 'A1'
UNION ALL SELECT 
    -- Special character in productId (Should be handled)
    202, '2024-03-01T00:00:00.000+0000', 50.0, 'B2'
UNION ALL SELECT 
    -- NULL handling
    102, NULL, 75.0, 'B2';

-- Create a temporary view for f_sales with diverse test cases
CREATE OR REPLACE TEMPORARY VIEW f_sales AS
SELECT 
    -- Happy path: valid sales data
    101 AS productId,
    '2024-01-15T00:00:00.000+0000' AS timestamp,
    10.0 AS salesQuantity,
    'A1' AS plantId
UNION ALL SELECT 
    -- Edge case: No sales
    101, '2024-02-01T00:00:00.000+0000', 0.0, 'A1'
UNION ALL SELECT 
    -- Error case: Negative sales
    101, '2024-02-15T00:00:00.000+0000', -3.0, 'A1'
UNION ALL SELECT 
    -- Special character in productId (Should be handled)
    202, '2024-03-01T00:00:00.000+0000', 5.0, 'B2'
UNION ALL SELECT 
    -- NULL handling
    102, NULL, NULL, 'B2';

-- Process data and calculate DOH
WITH AvgInventory AS (
    SELECT 
        productId,
        plantId,
        AVG(CASE WHEN inventoryQuantity IS NOT NULL THEN inventoryQuantity ELSE 0 END) AS avgInventory
    FROM f_inv_movmnt
    GROUP BY productId, plantId
),

AvgDailySales AS (
    SELECT
        productId,
        plantId,
        AVG(CASE WHEN salesQuantity IS NOT NULL THEN salesQuantity ELSE 0 END) AS avgDailySales
    FROM f_sales
    GROUP BY productId, plantId
)

-- Calculate DOH using previously computed averages
SELECT
    ai.productId,
    ai.plantId,
    ai.avgInventory,
    ads.avgDailySales,
    CASE 
        WHEN ads.avgDailySales = 0 THEN NULL -- Avoid division by zero
        ELSE (ai.avgInventory / ads.avgDailySales) * 365
    END AS DOH
FROM AvgInventory ai
JOIN AvgDailySales ads ON ai.productId = ads.productId AND ai.plantId = ads.plantId;
