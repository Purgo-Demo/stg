/* Test Code for Databricks SQL Environment */

/* Test: Calculate Average Inventory Over a Quarter */
SELECT
  item_nbr,
  plant_loc_cd,
  AVG(qty_on_hand) AS avg_inventory
FROM (
  SELECT * FROM purgo_playground.bien_schemaf_inv_movmnt WHERE flag_active = 'Y'
  UNION ALL
  SELECT * FROM purgo_playground.f_inv_movmnt WHERE flag_active = 'Y'
  UNION ALL
  SELECT * FROM purgo_playground.quang_schemaf_inv_movmnt WHERE flag_active = 'Y'
)
GROUP BY item_nbr, plant_loc_cd;

/* Test: Calculate Average Daily Sales Over a Quarter */
SELECT
  item_nbr,
  AVG(qty_sold) / 90.0 AS avg_daily_sales  -- Assuming 90 business days in a quarter
FROM (
  SELECT * FROM purgo_playground.bien_schemaf_sales WHERE flag_cancel = 'N' AND flag_return = 'N'
  UNION ALL
  SELECT * FROM purgo_playground.f_sales WHERE flag_cancel = 'N' AND flag_return = 'N'
  UNION ALL
  SELECT * FROM purgo_playground.quang_schemaf_sales WHERE flag_cancel = 'N' AND flag_return = 'N'
)
GROUP BY item_nbr;

/* Test: Calculate DOH for Each Item and Plant Location */
INSERT INTO purgo_playground.doh_metrics (item_nbr, avg_inventory, avg_daily_sales, doh)
SELECT
  inv.item_nbr,
  inv.avg_inventory,
  sales.avg_daily_sales,
  (inv.avg_inventory / sales.avg_daily_sales) * 365 AS doh
FROM (
  -- Calculate average inventory
  SELECT
    item_nbr,
    plant_loc_cd,
    AVG(qty_on_hand) AS avg_inventory
  FROM (
    SELECT * FROM purgo_playground.bien_schemaf_inv_movmnt WHERE flag_active = 'Y'
    UNION ALL
    SELECT * FROM purgo_playground.f_inv_movmnt WHERE flag_active = 'Y'
    UNION ALL
    SELECT * FROM purgo_playground.quang_schemaf_inv_movmnt WHERE flag_active = 'Y'
  )
  GROUP BY item_nbr, plant_loc_cd
) inv
JOIN (
  -- Calculate average daily sales
  SELECT
    item_nbr,
    AVG(qty_sold) / 90.0 AS avg_daily_sales  -- Assuming 90 business days in a quarter
  FROM (
    SELECT * FROM purgo_playground.bien_schemaf_sales WHERE flag_cancel = 'N' AND flag_return = 'N'
    UNION ALL
    SELECT * FROM purgo_playground.f_sales WHERE flag_cancel = 'N' AND flag_return = 'N'
    UNION ALL
    SELECT * FROM purgo_playground.quang_schemaf_sales WHERE flag_cancel = 'N' AND flag_return = 'N'
  )
  GROUP BY item_nbr
) sales
ON inv.item_nbr = sales.item_nbr;

/* Test: Validate DOH Metrics Calculation */
INSERT INTO purgo_playground.doh_kpis (avg_doh, stddev_doh)
SELECT
  AVG(doh) AS avg_doh,
  STDDEV(doh) AS stddev_doh
FROM purgo_playground.doh_metrics;

/* Test: Error Handling for Invalid Data Inputs */
-- Databricks SQL does not have direct mechanism for reporting validation error, but we can perform checks
SELECT
  ref_txn_id,
  item_nbr,
  qty_sold
FROM purgo_playground.bien_schemaf_sales
WHERE qty_sold < 0 OR qty_sold IS NULL
UNION ALL
SELECT
  ref_txn_id,
  item_nbr,
  qty_sold
FROM purgo_playground.f_sales
WHERE qty_sold < 0 OR qty_sold IS NULL
UNION ALL
SELECT
  ref_txn_id,
  item_nbr,
  qty_sold
FROM purgo_playground.quang_schemaf_sales
WHERE qty_sold < 0 OR qty_sold IS NULL;

/* Test: Handle Data from Multiple Schemas and Table Versions */
/* Assuming consistent data retrieval methods are set at the database level */
SELECT
  item_nbr,
  plant_loc_cd,
  AVG(qty_on_hand) AS avg_inventory
FROM (
  SELECT * FROM purgo_playground.bien_schemaf_inv_movmnt WHERE flag_active = 'Y'
  UNION ALL
  SELECT * FROM purgo_playground.f_inv_movmnt WHERE flag_active = 'Y'
  UNION ALL
  SELECT * FROM purgo_playground.quang_schemaf_inv_movmnt WHERE flag_active = 'Y'
)
GROUP BY item_nbr, plant_loc_cd;

