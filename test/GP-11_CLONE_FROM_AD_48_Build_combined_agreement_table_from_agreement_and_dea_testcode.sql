/*-----------------------------*/
/*  Test Code for Databricks   */
/*   Combined Agreement Table  */
/*-----------------------------*/

/* 
-- Setup or configuration info.
-- Ensure the required Spark SQL libraries are included.
-- Usage of Delta Lake operations conditioned on Databricks environment.
*/

-- NOTE: Ensure that the workspace has access to the necessary tables with accurate schema definitions

/*-----------------------------*/
/*     Step 1: Prepare Data    */
/*-----------------------------*/

-- Insert test data into the 'agreement' table
INSERT INTO purgo_playground.agreement (agree_no, agree_desc, agree_type, agree_type_desc, agree_from_dt, agree_end_dt, source_country, crt_dt, source_customer_id, master_agreement_type, material_number) VALUES
('AG001', 'Agreement 001', 'Type1', 'Description1', '2024-01-01', '2024-12-31', 'USA', '2023-11-15T12:00:00+0000', 'CUST001', 'MasterTypeA', 'MAT001'),
('AG002', 'Agreement 002', 'Type2', 'Description2', '2024-01-01', '2024-06-30', 'CAN', '2023-11-16T12:00:00+0000', 'CUST002', 'MasterTypeB', 'MAT002'),
('AG003', 'Agreement 003', 'Type3', 'Description3', '2024-12-31', '2024-01-01', 'MEX', '2023-11-17T12:00:00+0000', 'CUST003', 'MasterTypeC', 'MAT003'),
('AG004', 'Agreement 004', 'Type4', 'Description4', '2024-01-15', '2024-07-15', 'USA', '2023-11-18T12:00:00+0000', 'CUST004', 'MasterTypeD', '12345'),  -- Error Case: Non-string material number
('AG005', NULL, 'Type5', NULL, NULL, '2024-05-15', 'USA', '2023-11-19T12:00:00+0000', 'CUST005', 'MasterTypeE', 'MAT005');

-- Insert test data into the 'deal' table
INSERT INTO purgo_playground.deal (deal, agree_type, material_number, master_agreement_type, source_customer_id) VALUES
('D001', 'Type1', 'MAT001', 'MasterTypeA', 'CUST001'),
('D002', 'Type2', 'MAT002', 'MasterTypeB', 'CUST002'),
('D003', 'TypeX', 'MAT003', 'MasterTypeC', 'CUST003'), -- Error Case: Inconsistent agree_type
('D004', NULL, 'MAT004', 'MasterTypeD', 'CUST004');  -- Null Handling

-- Insert test data into the 'customer' table
INSERT INTO purgo_playground.customer (source_customer_id, address1, state, city, postal_code) VALUES
('CUST001', '123 Main St', 'CA', 'Los Angeles', '90001'),
('CUST002', '456 Market St', 'NY', 'New York', '10001'),
('CUST003', '789 Especial Blvd', 'TX', 'Austin', '78701'),
('CUST004', '1010 Null Ave', NULL, 'Null City', '00000');

/*-----------------------------*/
/*   Step 2: Schema Validation */
/*-----------------------------*/

-- Check data type consistency for 'agreement' table
SELECT
  COUNT(*) AS invalid_material_number_count
FROM
  purgo_playground.agreement
WHERE
  TRY_CAST(material_number AS STRING) IS NULL;

SELECT
  COUNT(*) AS invalid_crt_dt_count
FROM
  purgo_playground.agreement
WHERE
  TRY_CAST(crt_dt AS TIMESTAMP) IS NULL;

/*-----------------------------*/
/*    Step 3: Join Operations  */
/*-----------------------------*/

-- Generate `combined_agreement` table using proper joins
CREATE OR REPLACE TABLE purgo_playground.combined_agreement AS
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
  a.source_customer_id = c.source_customer_id;

/*-----------------------------*/
/*   Step 4: Validation Tests  */
/*-----------------------------*/

-- Test for missing data
SELECT COUNT(*) AS missing_data_count
FROM purgo_playground.agreement a
LEFT JOIN purgo_playground.customer c ON a.source_customer_id = c.source_customer_id
WHERE c.source_customer_id IS NULL;

-- Validate field formats and non-string material number
SELECT COUNT(*) AS invalid_format_count
FROM purgo_playground.agreement
WHERE TRY_CAST(material_number AS STRING) IS NULL;

/*-----------------------------*/
/*     Step 5: Data Cleanup    */
/*-----------------------------*/

-- Ensure cleanup of test data if persistent tables were used (not shown)
-- This is left as commented for safety in actual test scenarios
-- DELETE FROM purgo_playground.agreement WHERE agree_no IN ('AG_UNIQUE_TEST');
-- DELETE FROM purgo_playground.deal WHERE deal IN ('D_UNIQUE_TEST');
-- DELETE FROM purgo_playground.customer WHERE source_customer_id IN ('CUST_TEST');
