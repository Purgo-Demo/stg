-- SQL Implementation for Calculating Inventory at Risk

/* Calculate inventory at risk based on DNSA flag and active status */
WITH InventoryRisk AS (
  SELECT 
    SUM(CASE WHEN flag_active = 'Y' AND dnsa_flag = 'yes' THEN financial_qty ELSE 0 END) AS inventory_at_risk
  FROM purgo_playground.f_inv_movmnt
),
/* Calculate total inventory to determine percentage at risk */
TotalInventory AS (
  SELECT 
    SUM(financial_qty) AS total_inventory
  FROM purgo_playground.f_inv_movmnt
)
/* Final calculation of percentage of inventory at risk */
SELECT 
  ir.inventory_at_risk,
  ti.total_inventory,
  CASE 
    WHEN ti.total_inventory = 0 THEN 0 
    ELSE (ir.inventory_at_risk / ti.total_inventory) * 100 
  END AS inventory_at_risk_percentage
FROM 
  InventoryRisk ir,
  TotalInventory ti;

/* Handling potential data issues */
-- Validation of possible NULL scenarios or errors
SELECT
  CASE 
    WHEN SUM(CASE WHEN flag_active IS NULL OR dnsa_flag IS NULL OR financial_qty IS NULL THEN 1 ELSE 0 END) = 0 
    THEN 'No NULLs or issues with DNSA flags or Active flags'
    ELSE 'Issues detected: Check for NULLs or Data Quality'
  END AS data_quality_check
FROM 
  purgo_playground.f_inv_movmnt;

/* Additional selection to observe recorded errors or pattern checks */
-- Investigate inventory entries without active flags or proper DNSA flags
SELECT
  txn_id,
  inv_loc,
  financial_qty,
  flag_active,
  dnsa_flag 
FROM 
  purgo_playground.f_inv_movmnt
WHERE 
  flag_active IS NULL 
  OR dnsa_flag IS NULL 
  OR financial_qty IS NULL
  OR flag_active != 'Y'
  OR dnsa_flag NOT IN ('yes', 'no');

/* Example of Delta Lake usage for time travel if applicable */
-- SELECT * FROM purgo_playground.f_inv_movmnt TIMESTAMP AS OF date_sub(current_timestamp(), 1)
-- Note: The above line is commented out as a placeholder for time travel use within Delta Lake

-- Ensure table optimizations for Z-ordering, clustering, and vacuum processes can be executed appropriately
