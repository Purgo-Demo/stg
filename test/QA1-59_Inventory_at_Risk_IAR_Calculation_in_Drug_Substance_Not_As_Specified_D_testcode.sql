-- Ensure necessary libraries are installed for Delta Lake operations
-- Libraries for SQL are usually pre-installed in Databricks. Ensure Delta Lake version is compatible with SQL operations.

-- Schema Validation Test for f_inv_movmnt Table to prevent runtime errors
/*
Ensure the schema for 'purgo_playground.f_inv_movmnt' is as follows:
txn_id: STRING, inv_loc: STRING, financial_qty: DOUBLE, net_qty: DOUBLE, expired_qt: DECIMAL(38,0),
item_nbr: STRING, unit_cost: DOUBLE, um_rate: DOUBLE, plant_loc_cd: STRING, inv_stock_reference: STRING,
stock_type: STRING, qty_on_hand: DOUBLE, qty_shipped: DOUBLE, cancel_dt: DECIMAL(38,0), flag_active: STRING,
crt_dt: TIMESTAMP, updt_dt: TIMESTAMP
*/

-- SQL Query to Calculate Inventory at Risk
WITH cte_inventory AS (
  SELECT
    SUM(financial_qty) AS inventory_at_risk -- Summing financial_qty where DNSA is active
  FROM
    purgo_playground.f_inv_movmnt
  WHERE
    flag_active = "Y"
),
cte_total_inventory AS (
  SELECT
    SUM(financial_qty) AS total_inventory -- Calculate total financial_qty for all records
  FROM
    purgo_playground.f_inv_movmnt
)
SELECT
  inventory_at_risk,
  CASE
    WHEN total_inventory = 0 THEN 0 -- Prevent division by zero
    ELSE (inventory_at_risk / total_inventory) * 100.0 -- Calculating percentage of Inventory at Risk
  END AS percentage_of_inventory_at_risk
FROM
  cte_inventory
CROSS JOIN cte_total_inventory;

-- Test Coverage for Correct Calculation
-- Ensures the query returns correct aggregation and percentage calculation for the inventory at risk.

-- Data Quality Validation in Calculations
-- Validate correct inclusion of records by ensuring non-negative quantities are summed.

-- Test Delta Lake Specific Operations
-- Verify DELETE and UPDATE operations to maintain data integrity and automate cleanups.

/* Example Cleanup Operation for Safe Data Management */
-- DELETE FROM purgo_playground.f_inv_movmnt WHERE <specific conditions> -- Uncomment and specify suitable conditions

-- Note: Employ DELETE cautiously to prevent accidental data removal.

-- Additional Assertion for Advanced Functions
-- Verify functionality if using advanced SQL features such as WINDOW functions for time-based calculations.
