-- Test Data Generation for Databricks SQL

-- Create the `f_inv_movmnt` table schema
CREATE TABLE IF NOT EXISTS f_inv_movmnt (
  productId STRING,
  plantId STRING,
  movementDate TIMESTAMP,
  inventory BIGINT,
  comments STRING
);

-- Insert test data into `f_inv_movmnt`
-- Happy path and edge cases
INSERT INTO f_inv_movmnt VALUES
('101', 'A1', '2024-03-01T00:00:00.000+0000', 1500, 'Initial stock'),
('101', 'A1', '2024-03-30T00:00:00.000+0000', 1300, 'Sold some products'),
('102', 'B2', '2024-03-15T00:00:00.000+0000', 0, 'Out of stock'), -- Edge case: Inventory zero
('103', 'C3', '2024-04-01T00:00:00.000+0000', 2000, NULL), -- NULL comment handling
('104', 'D4', '2024-03-01T00:00:00.000+0000', 2500, 'Restock with special characters !@#$%^&*()'),
('105', 'E5', '2024-03-10T00:00:00.000+0000', 700, 'Update ©®∆ƒ'); -- Special Unicode characters

-- Create the `f_sales` table schema
CREATE TABLE IF NOT EXISTS f_sales (
  productId STRING,
  plantId STRING,
  saleDate TIMESTAMP,
  salesQuantity BIGINT,
  comments STRING
);

-- Insert test data into `f_sales`
-- Happy path and edge cases
INSERT INTO f_sales VALUES
('101', 'A1', '2024-03-05T00:00:00.000+0000', 300, 'First week sales'),
('101', 'A1', '2024-03-21T00:00:00.000+0000', 400, 'Mid-March sales'),
('102', 'B2', '2024-03-10T00:00:00.000+0000', -50, 'Returned items'), -- Error case: Negative sales
('103', 'C3', '2024-03-25T00:00:00.000+0000', 600, NULL), -- NULL comment handling
('104', 'D4', '2024-03-15T00:00:00.000+0000', 800, 'Peak sales with special characters !@#$%^&*()'),
('105', 'E5', '2024-03-30T00:00:00.000+0000', 0, 'End of month sales check'); -- Boundary case: Sales zero

-- Error cases for missing data scenarios
-- Create empty tables to simulate missing data
CREATE TABLE IF NOT EXISTS f_inv_movmnt_missing (
  productId STRING,
  plantId STRING,
  movementDate TIMESTAMP,
  inventory BIGINT,
  comments STRING
);

CREATE TABLE IF NOT EXISTS f_sales_missing (
  productId STRING,
  plantId STRING,
  saleDate TIMESTAMP,
  salesQuantity BIGINT,
  comments STRING
);

-- No insertions into these tables to simulate missing data scenarios

-- Add comments explaining test scenarios in SQL script
-- This script is designed to validate DOH calculations with various scenarios, including: 
-- Happy path calculations, inventory/sales zero or negative, missing data, NULL handling, special chars, and Unicode.

