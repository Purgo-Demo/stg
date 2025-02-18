/*-----------------------------*/
/*    Combined Agreement Table */
/*   Implementation in SQL     */
/*-----------------------------*/

/* 
-- Setup or configuration info.
-- Ensure the required SQL libraries are included.
-- Usage of Delta Lake operations conditioned on Databricks environment.
*/

-- NOTE: Ensure that the workspace has access to the necessary tables with accurate schema definitions

/*-----------------------------*/
/*       Step 1: Preparation   */
/*-----------------------------*/

/* Prepare necessary tables */

/*-----------------------------*/
/*       Step 2: Execute Join  */
/*-----------------------------*/

-- Create the `combined_agreement` table
CREATE OR REPLACE TABLE purgo_playground.combined_agreement
USING DELTA
AS
SELECT
  a.agree_no,
  a.agree_desc,
  a.agree_type,
  a.material_number,
  a.master_agreement_type,
  c.source_customer_id,
  c.address1,
  c.state,
  c.city,
  c.postal_code
FROM
  purgo_playground.agreement a
JOIN
  purgo_playground.customer c 
ON 
  a.source_customer_id = c.source_customer_id
UNION
SELECT
  d.deal AS agree_no,
  NULL AS agree_desc,
  d.agree_type,
  d.material_number,
  d.master_agreement_type,
  c.source_customer_id,
  c.address1,
  c.state,
  c.city,
  c.postal_code
FROM
  purgo_playground.deal d
JOIN
  purgo_playground.customer c 
ON 
  d.source_customer_id = c.source_customer_id;

/*-----------------------------*/
/* Step 3: Handling Duplicates */
/*-----------------------------*/

-- To manage duplicates, ensure uniqueness in the combined table
CREATE OR REPLACE TABLE purgo_playground.combined_agreement
USING DELTA
AS
SELECT DISTINCT
  *
FROM purgo_playground.combined_agreement;

/*-----------------------------*/
/* Step 4: Performance Tuning  */
/*-----------------------------*/

-- Optimize the table for specific query patterns (correct syntax)
OPTIMIZE purgo_playground.combined_agreement
ZORDER BY source_customer_id, material_number;

/*-----------------------------*/
/*   Step 5: Data Validation   */
/*-----------------------------*/

-- Validate consistency post-merge operation
SELECT
  COUNT(*) AS total_records,
  COUNT(DISTINCT source_customer_id) AS distinct_customers
FROM
  purgo_playground.combined_agreement;

/* Validate no NULL critical fields */
SELECT 
  COUNT(*) AS null_agree_no_count
FROM 
  purgo_playground.combined_agreement
WHERE 
  agree_no IS NULL;

SELECT 
  COUNT(*) AS null_source_customer_id_count
FROM 
  purgo_playground.combined_agreement
WHERE 
  source_customer_id IS NULL;

/*-----------------------------*/
/*     Cleanup Data if needed  */
/*-----------------------------*/

-- Uncomment to delete test records if necessary
-- DELETE FROM purgo_playground.agreement WHERE agree_no IN ('AG_UNIQUE_TEST');
-- DELETE FROM purgo_playground.deal WHERE deal IN ('D_UNIQUE_TEST');
-- DELETE FROM purgo_playground.customer WHERE source_customer_id IN ('CUST_TEST');
