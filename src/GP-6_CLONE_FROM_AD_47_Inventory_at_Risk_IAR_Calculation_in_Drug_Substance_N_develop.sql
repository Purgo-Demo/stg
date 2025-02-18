-- Use the designated schema from Unity Catalog
USE SCHEMA purgo_playground;

-- Create a temporary view for ease of use in calculations
CREATE OR REPLACE TEMPORARY VIEW f_inv_movmnt_view AS
SELECT *
FROM purgo_playground.f_inv_movmnt;

-- Calculation of Inventory at Risk and Percentage of Inventory at Risk
-- including handling of possible NULLs and ensuring type consistency
WITH inventory_at_risk AS (
  SELECT COALESCE(SUM(financial_qty), 0) AS inventory_at_risk -- Handle possible NULLs
  FROM f_inv_movmnt_view
  WHERE flag_active = 'Y'
),
total_inventory AS (
  SELECT COALESCE(SUM(financial_qty), 0) AS total_inventory -- Handle possible NULLs
  FROM f_inv_movmnt_view
)
SELECT inventory_at_risk.inventory_at_risk, 
  total_inventory.total_inventory, 
  CASE WHEN total_inventory.total_inventory > 0 THEN 
    (inventory_at_risk.inventory_at_risk / total_inventory.total_inventory * 100) 
  ELSE 
    0.0 
  END AS percentage_inventory_at_risk -- Handle division by zero
FROM inventory_at_risk 
CROSS JOIN total_inventory;

-- Create a Delta table for storing inventory at risk data
CREATE OR REPLACE TABLE delta.inventory_risk
USING delta
AS
SELECT *,
  current_timestamp() AS calculation_time -- Add timestamp for tracking
FROM (
  SELECT inventory_at_risk.inventory_at_risk, 
    total_inventory.total_inventory, 
    CASE WHEN total_inventory.total_inventory > 0 THEN 
      (inventory_at_risk.inventory_at_risk / total_inventory.total_inventory * 100) 
    ELSE 
      0.0 
    END AS percentage_inventory_at_risk
  FROM inventory_at_risk 
  CROSS JOIN total_inventory
);

-- Optimize the delta table
-- Z-Ordering by inventory_at_risk and percentage_inventory_at_risk may improve query performance
OPTIMIZE delta.inventory_risk
ZORDER BY (inventory_at_risk, percentage_inventory_at_risk);

-- Vacuum the table to remove old files
-- Consider the compliance and data retention policies before using
VACUUM delta.inventory_risk RETAIN 0 HOURS;

-- Cleanup temporary views
DROP VIEW IF EXISTS f_inv_movmnt_view;
