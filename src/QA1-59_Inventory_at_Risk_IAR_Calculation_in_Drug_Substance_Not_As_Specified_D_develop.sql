-- Calculate Inventory at Risk in Real-Time Based on DNSA Flags

-- CTE for calculating inventory at risk where DNSA flag is active
WITH cte_inventory_at_risk AS (
  SELECT
    SUM(financial_qty) AS inventory_at_risk
  FROM
    purgo_playground.f_inv_movmnt
  WHERE
    flag_active = "Y"
),

-- CTE for calculating total inventory
cte_total_inventory AS (
  SELECT
    SUM(financial_qty) AS total_inventory
  FROM
    purgo_playground.f_inv_movmnt
)

-- Final selection calculating inventory at risk and its percentage
SELECT
  cte_inventory_at_risk.inventory_at_risk,
  CASE
    WHEN cte_total_inventory.total_inventory = 0 THEN 0  -- Handling division by zero
    ELSE (cte_inventory_at_risk.inventory_at_risk / cte_total_inventory.total_inventory) * 100.0 -- Inventory at risk percentage
  END AS percentage_of_inventory_at_risk
FROM
  cte_inventory_at_risk
CROSS JOIN
  cte_total_inventory;

-- Note: Implement Delta Lake optimization and cleanup logic in separate transaction blocks
-- Example: Optimize Delta tables and utilize ZORDER BY on high-cardinality fields like item_nbr, flag_active

-- Error Handling and Logging
-- Implement checks for missing data or incorrect flags and apply logging for audit trails

-- Delta Lake Integration
-- Use Delta Lake features like MERGE for updates and VACUUM for managing old versions
-- Utilize time travel features to audit changes

-- Data Validation
-- Validate schema and ensure that financial quantities are positive
-- Confirm accurate summation and aggregation of inventory values

-- Documentation
-- Provide SQL block comments for section purposes and complex logic explanations

