-- Create the table schema for test data
CREATE OR REPLACE TABLE purgo_playground.doh_test_data (
  quarter STRING,
  average_inventory BIGINT,
  average_daily_sales DOUBLE,
  doh DOUBLE,
  average_doh DOUBLE,
  std_dev DOUBLE
);

-- Happy path test data
INSERT INTO purgo_playground.doh_test_data VALUES
('Q1-2024', 10000, 100.0, 36500.0, 36500.0, 0.0), -- Valid data
('Q2-2024', 5000, 50.0, 36500.0, 36500.0, 0.0);

-- Edge case test data
INSERT INTO purgo_playground.doh_test_data VALUES
('End-Q1-2024', 0, 100.0, 0.0, NULL, NULL), -- Zero inventory
('Start-Q3-2024', 10000, 0.1, 3650000.0, NULL, NULL); -- Very low sales

-- Error case test data
-- Invalid average daily sales (zero)
INSERT INTO purgo_playground.doh_test_data VALUES
('Mid-Q2-2024', 10000, 0.0, NULL, NULL, NULL);

-- NULL handling scenarios
-- NULL average inventory
INSERT INTO purgo_playground.doh_test_data VALUES
('Null-Q3-2024', NULL, 100.0, NULL, NULL, NULL);

-- Special characters and multi-byte characters
INSERT INTO purgo_playground.doh_test_data VALUES
('Q4-2024ów!', 12345, 123.45, 36599.18, 36599.18, 0.0), -- Special and multi-byte characters in quarter

-- Validation for error messages
-- Negative values for inventory and sales
INSERT INTO purgo_playground.doh_test_data VALUES
('Negative-Q2-2024', -1000, 100.0, NULL, NULL, NULL),
('Negative-Q4-2024', 10000, -100.0, NULL, NULL, NULL);

-- Output stored with DOH calculations
CREATE OR REPLACE TABLE purgo_playground.doh_results AS
SELECT
  quarter,
  average_inventory,
  average_daily_sales,
  (average_inventory / average_daily_sales) * 365 AS doh
FROM purgo_playground.doh_test_data
WHERE average_daily_sales > 0 AND average_inventory IS NOT NULL;

-- Output specifications storing results
ALTER TABLE purgo_playground.doh_results
ADD COLUMNS (average_doh DOUBLE, std_dev DOUBLE);

-- Sample output for demonstration
SELECT * FROM purgo_playground.doh_results;
