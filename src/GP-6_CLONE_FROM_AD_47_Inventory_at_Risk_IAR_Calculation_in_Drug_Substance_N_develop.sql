-- Inventory at Risk Calculation based on DNSA Events

-- Create a temporary view to calculate the total inventory
CREATE OR REPLACE TEMP VIEW total_inventory_view AS
SELECT 
  SUM(financial_qty) AS total_inventory
FROM 
  purgo_playground.f_inv_movmnt; 

-- Calculate the inventory_at_risk and percentage_of_inventory_at_risk
SELECT 
  inventory_at_risk_calculation.inventory_at_risk,
  (inventory_at_risk_calculation.inventory_at_risk / total_inventory_view.total_inventory) * 100 AS percentage_of_inventory_at_risk
FROM (
  SELECT 
    SUM(CASE WHEN flag_active = 'Y' THEN financial_qty ELSE 0 END) AS inventory_at_risk
  FROM 
    purgo_playground.f_inv_movmnt
  WHERE 
    flag_active = 'Y'  -- Process only active flags
) inventory_at_risk_calculation,
total_inventory_view;

-- Implement error handling for missing DNSA flag
-- Log and exclude records with missing or null DNSA flags
SELECT
  txn_id,
  inv_loc,
  financial_qty,
  flag_active,
  CASE 
    WHEN financial_qty IS NULL THEN "Missing financial_qty in record" 
    WHEN flag_active IS NULL THEN "Missing flag_active in record" 
    ELSE NULL 
  END AS error_message
FROM 
  purgo_playground.f_inv_movmnt
WHERE 
  financial_qty IS NULL OR flag_active IS NULL;

-- Example of data skew handling strategy
-- Order by keys predicted to reduce skew for specific queries
-- This is an example and might need adjustments based on actual data distribution
SELECT 
  /*+ REPARTITION(100, plant_loc_cd) */  -- Use plant_loc_cd to mitigate skew
  inv_loc, 
  item_nbr, 
  SUM(financial_qty) AS total_qty
FROM 
  purgo_playground.f_inv_movmnt
WHERE 
  flag_active = 'Y'
GROUP BY 
  inv_loc, item_nbr
ORDER BY 
  total_qty DESC;
