-- Step 1: Prepare Test Data for 'agreement' table
INSERT INTO purgo_playground.agreement (agree_no, agree_desc, agree_type, agree_type_desc, agree_from_dt, agree_end_dt, source_country, crt_dt, source_customer_id, master_agreement_type, material_number) VALUES
-- Happy Path
('AG001', 'Agreement 001', 'Type1', 'Description1', '2024-01-01', '2024-12-31', 'USA', '2023-11-15T12:00:00.000+0000', 'CUST001', 'MasterTypeA', 'MAT001'),
('AG002', 'Agreement 002', 'Type2', 'Description2', '2024-01-01', '2024-06-30', 'CAN', '2023-11-16T12:00:00.000+0000', 'CUST002', 'MasterTypeB', 'MAT002'),
-- Edge Case: End date before start date
('AG003', 'Agreement 003', 'Type3', 'Description3', '2024-12-31', '2024-01-01', 'MEX', '2023-11-17T12:00:00.000+0000', 'CUST003', 'MasterTypeC', 'MAT003'),
-- Error Case: Non-string material number
('AG004', 'Agreement 004', 'Type4', 'Description4', '2024-01-15', '2024-07-15', 'USA', '2023-11-18T12:00:00.000+0000', 'CUST004', 'MasterTypeD', 12345),
-- Null handling
('AG005', NULL, 'Type5', NULL, NULL, '2024-05-15', 'USA', '2023-11-19T12:00:00.000+0000', 'CUST005', 'MasterTypeE', 'MAT005');

-- Step 2: Prepare Test Data for 'deal' table
INSERT INTO purgo_playground.deal (deal, agree_type, material_number, master_agreement_type, source_customer_id) VALUES
-- Happy Path
('D001', 'Type1', 'MAT001', 'MasterTypeA', 'CUST001'),
('D002', 'Type2', 'MAT002', 'MasterTypeB', 'CUST002'),
-- Error Case: Inconsistent agree_type
('D003', 'TypeX', 'MAT003', 'MasterTypeC', 'CUST003'),
-- Null Handling
('D004', NULL, 'MAT004', 'MasterTypeD', 'CUST004');

-- Step 3: Prepare Test Data for 'customer' table
INSERT INTO purgo_playground.customer (source_customer_id, address1, state, city, postal_code) VALUES
-- Happy Path
('CUST001', '123 Main St', 'CA', 'Los Angeles', '90001'),
('CUST002', '456 Market St', 'NY', 'New York', '10001'),
-- Special Characters
('CUST003', '789 Especial Blvd', 'TX', 'Austin', '78701'),
-- Null Handling
('CUST004', '1010 Null Ave', NULL, 'Null City', '00000');

-- Step 4: Generate SQL for combined_agreement table
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
  purgo_playground.customer c ON a.source_customer_id = c.source_customer_id
WHERE
  a.agree_no IS NOT NULL
AND NOT EXISTS (
  SELECT 1 FROM purgo_playground.deal d
  WHERE a.material_number = d.material_number
  AND a.agree_type = d.agree_type
  AND a.source_customer_id = d.source_customer_id
);

-- Step 5: Generate Error Scenarios
-- Handle Missing Data
SELECT * FROM purgo_playground.agreement a
LEFT JOIN purgo_playground.customer c ON a.source_customer_id = c.source_customer_id
WHERE c.source_customer_id IS NULL;

-- Validate Field Formats
SELECT * FROM purgo_playground.agreement
WHERE TRY_CAST(material_number AS STRING) IS NULL;

-- Handle Duplicate Records
SELECT source_customer_id, COUNT(*) 
FROM 
  (SELECT * FROM purgo_playground.agreement 
   UNION ALL 
   SELECT * FROM purgo_playground.deal) 
GROUP BY source_customer_id 
HAVING COUNT(*) > 1;
