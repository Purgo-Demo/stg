-- Databricks SQL Query for Extracting Fields from product_details JSON String
SELECT
  product_id,
  product_name,
  -- Extract batch_number from JSON and cast as STRING
  CAST(JSON_VALUE(product_details, '$.batch_number') AS STRING) AS batch_number,
  -- Extract expiration_date from JSON and cast as STRING
  CAST(JSON_VALUE(product_details, '$.expiration_date') AS STRING) AS expiration_date,
  -- Extract manufacturing_site from JSON and cast as STRING
  CAST(JSON_VALUE(product_details, '$.manufacturing_site') AS STRING) AS manufacturing_site,
  -- Extract regulatory_approval from JSON and cast as STRING
  CAST(JSON_VALUE(product_details, '$.regulatory_approval') AS STRING) AS regulatory_approval,
  -- Extract price from JSON and cast as DOUBLE
  CAST(JSON_VALUE(product_details, '$.price') AS DOUBLE) AS price
FROM
  purgo_playground.d_product_revenue;

-- Generate test data with various scenarios

-- Happy Path
INSERT INTO purgo_playground.d_product_revenue
VALUES 
  (1, 'Product A', '{"batch_number": "BATCH2024-5678", "expiration_date": "2025-12-31", "manufacturing_site": "Site A", "regulatory_approval": "Approved", "price": 250.75}');

-- Edge Cases
-- 1. Maximum value for price
INSERT INTO purgo_playground.d_product_revenue
VALUES 
  (2, 'Product B', '{"batch_number": "BATCH2024-9999", "expiration_date": "9999-12-31", "manufacturing_site": "Site Z", "regulatory_approval": "Approved", "price": 999999999.99}');

-- Error Cases
-- 1. Invalid price
INSERT INTO purgo_playground.d_product_revenue
VALUES 
  (3, 'Product C', '{"batch_number": "BATCH2024-1234", "expiration_date": "2024-12-31", "manufacturing_site": "Site B", "regulatory_approval": "Approved", "price": "invalid_price"}');

-- NULL Handling Scenarios
-- 1. Null product_details
INSERT INTO purgo_playground.d_product_revenue
VALUES 
  (4, 'Product D', NULL);

-- 2. Missing JSON field
INSERT INTO purgo_playground.d_product_revenue
VALUES 
  (5, 'Product E', '{"batch_number": "BATCH2024-5678", "expiration_date": null, "manufacturing_site": "Site D", "regulatory_approval": "Approved", "price": 150.50}');

-- Special Characters / Multi-byte Characters
-- 1. Special characters in manufacturing site
INSERT INTO purgo_playground.d_product_revenue
VALUES 
  (6, 'Product F', '{"batch_number": "BATCH2024-5678", "expiration_date": "2024-12-31", "manufacturing_site": "Site &!@#", "regulatory_approval": "Approved", "price": 200.00}');

-- 2. Multi-byte characters in manufacturing site
INSERT INTO purgo_playground.d_product_revenue
VALUES 
  (7, 'Product G', '{"batch_number": "BATCH2024-5678", "expiration_date": "2026-01-01", "manufacturing_site": "サイトA", "regulatory_approval": "Approved", "price": 300.00}');
