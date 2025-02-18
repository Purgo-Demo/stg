/* Calculate allocated_qty for each order */

-- This query calculates the allocated_qty for each order in the f_order table
-- It handles null values by using COALESCE to treat them as 0 for the calculation.

WITH allocated_qty_data AS (
    SELECT
        order_nbr,
        order_line_nbr,
        COALESCE(primary_qty, 0.0) + COALESCE(open_qty, 0.0) + COALESCE(shipped_qty, 0.0) + COALESCE(cancel_qty, 0.0) AS allocated_qty
    FROM 
        purgo_playground.f_order
)
SELECT
    order_nbr,
    order_line_nbr,
    allocated_qty
FROM
    allocated_qty_data
ORDER BY
    order_nbr,
    order_line_nbr;

/* Handle any modifications to update allocated_qty dynamically as quantities change */

-- Example MERGE to update allocated_qty when a new shipped_qty is recorded
MERGE INTO purgo_playground.f_order AS target
USING (
    SELECT
        order_nbr,
        order_line_nbr,
        primary_qty,
        open_qty,
        new_shipped_qty AS shipped_qty,
        cancel_qty
    FROM 
        purgo_playground.f_order
    WHERE
        order_nbr = 'ORDR006' AND order_line_nbr = '001'
) AS source
ON target.order_nbr = source.order_nbr AND target.order_line_nbr = source.order_line_nbr
WHEN MATCHED THEN
    UPDATE SET 
        target.shipped_qty = source.shipped_qty,
        target.primary_qty = source.primary_qty,
        target.open_qty = source.open_qty,
        target.cancel_qty = source.cancel_qty;

/* Error checking: Alert when no matching entry is found */
SELECT 
    CASE WHEN COUNT(1) = 0 THEN 'Error: Order number or line number not found in f_order table.'
         ELSE 'Entry exists' END AS existence_check
FROM purgo_playground.f_order
WHERE order_nbr = 'ORDR005'
  AND order_line_nbr = '003';

/* Apply ZORDER optimization to improve query performance on specific columns, executed separately */
-- Using Delta Lake's OPTIMIZE command to enhance query performance
-- Ensure appropriate permissions for running OPTIMIZE commands

-- OPTIMIZE purgo_playground.f_order
--   ZORDER BY (order_nbr, order_line_nbr);

-- Note: Execute OPTIMIZE outside of this script, as it may not be supported inline

/* Vacuum the Delta table to remove old data and retain necessary data, executed separately */
-- The VACUUM command ensures data housekeeping in a Delta table

-- VACUUM purgo_playground.f_order RETAIN 168 HOURS;

-- Note: Execute VACUUM outside of this script, adapt retention hours as needed

