-- Comprehensive SQL Test Code for Databricks Environment

/* 
   SQL Code to validate that 'delivery_dt' in the 'f_order' table 
   is 100% Decimal (38,0) and in 'yyyymmdd' format 
*/

-- Test for valid Decimal and Date Format in 'f_order' table
WITH delivery_date_checks AS (
  SELECT
    order_nbr,
    delivery_dt,
    -- Check if delivery_dt is within valid 'yyyymmdd' range Format
    CASE 
      WHEN CAST(delivery_dt AS STRING) RLIKE '^[0-9]{8}$'
        AND (delivery_dt BETWEEN 10000101 AND 99991231)
      THEN 1
      ELSE 0
    END AS is_valid_format,
    -- Check if delivery_dt is NOT NULL and is of type Decimal(38,0)
    CASE 
      WHEN delivery_dt IS NOT NULL 
        AND CAST(delivery_dt AS DECIMAL(38,0)) = delivery_dt
      THEN 1
      ELSE 0
    END AS is_valid_decimal
  FROM agilisium_playground.purgo_playground.f_order
)
SELECT
  COUNT(*) AS total_records,
  SUM(is_valid_format) AS valid_format_count,
  SUM(is_valid_decimal) AS valid_decimal_count,
  -- Assert that all records should have valid format and type
  CASE 
    WHEN SUM(is_valid_format) = COUNT(*) 
      AND SUM(is_valid_decimal) = COUNT(*)
    THEN 'PASS'
    ELSE 'FAIL'
  END AS validation_result
FROM delivery_date_checks;

/* 
   Test Clean-up operations 
   Drop temporary views or tables created during testing 
*/
DROP VIEW IF EXISTS agilisium_playground.test_delivery_date_check;
