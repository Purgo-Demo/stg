-- Databricks SQL Query for Extracting Fields from product_details JSON String
SELECT
  product_id,
  product_name,
  -- Extract and cast batch_number from JSON
  CAST(JSON_VALUE(product_details, '$.batch_number') AS STRING) AS batch_number,
  -- Extract and cast expiration_date from JSON
  CAST(JSON_VALUE(product_details, '$.expiration_date') AS STRING) AS expiration_date,
  -- Extract and cast manufacturing_site from JSON
  CAST(JSON_VALUE(product_details, '$.manufacturing_site') AS STRING) AS manufacturing_site,
  -- Extract and cast regulatory_approval from JSON
  CAST(JSON_VALUE(product_details, '$.regulatory_approval') AS STRING) AS regulatory_approval,
  -- Extract and cast price from JSON
  CAST(JSON_VALUE(product_details, '$.price') AS DOUBLE) AS price
FROM
  purgo_playground.d_product_revenue
-- Handle missing values by ensuring nullability in case JSON fields are absent or JSON parsing fails
-- Data quality checks for valid JSON format can be handled upstream or with additional checks


