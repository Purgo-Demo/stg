/* Update the d_product_vw view to include the hfm_entity column with data populated for 'orafin' source system code */

-- Create or replace the view in purgo_playground schema
CREATE OR REPLACE VIEW purgo_playground.d_product_vw AS
SELECT
  dp.prod_id,
  dp.item_nbr,
  dp.unit_cost,
  dp.prod_exp_dt,
  dp.cost_per_pkg,
  dp.plant_add,
  dp.plant_loc_cd,
  dp.prod_line,
  dp.stock_type,
  dp.pre_prod_days,
  dp.sellable_qty,
  dp.prod_ordr_tracker_nbr,
  dp.max_order_qty,
  dp.flag_active,
  dp.crt_dt,
  dp.updt_dt,
  -- Populate hfm_entity by joining with edp_lkup only for src_sys_cd = 'orafin'
  CASE
    WHEN dp.src_sys_cd = 'orafin' THEN el.lkkup_val_01 
    ELSE dp.hfm_entity 
  END AS hfm_entity
FROM
  purgo_playground.d_product dp
LEFT JOIN
  purgo_playground.edp_lkup el
ON
  dp.stock_type = el.lkup_key_01;
