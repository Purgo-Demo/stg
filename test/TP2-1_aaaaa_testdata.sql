-- Creating test data tables for f_inv_movmnt and f_sales using Databricks SQL

-- Schema for f_inv_movmnt
CREATE OR REPLACE TABLE f_inv_movmnt (
  productId STRING,
  plantId STRING,
  movementDate TIMESTAMP,
  inventoryLevel BIGINT
);

-- Schema for f_sales
CREATE OR REPLACE TABLE f_sales (
  productId STRING,
  plantId STRING,
  salesDate TIMESTAMP,
  salesVolume BIGINT
);

-- Inserting test data for happy path, edge cases, error cases, and NULL scenarios

-- Happy path test data: Valid inventory and sales data
INSERT INTO f_inv_movmnt VALUES
('101', 'A1', '2024-01-01T00:00:00.000+0000', 500),
('102', 'B2', '2024-01-01T00:00:00.000+0000', 600);

INSERT INTO f_sales VALUES
('101', 'A1', '2024-01-01T00:00:00.000+0000', 50),
('102', 'B2', '2024-01-01T00:00:00.000+0000', 30);

-- Edge case: Minimum and maximum inventory and sales data
INSERT INTO f_inv_movmnt VALUES
('101', 'A1', '2024-01-01T00:00:00.000+0000', 0),
('102', 'B2', '2024-01-01T00:00:00.000+0000', 9223372036854775807); -- Max BIGINT

INSERT INTO f_sales VALUES
('101', 'A1', '2024-01-01T00:00:00.000+0000', 1),
('102', 'B2', '2024-01-01T00:00:00.000+0000', 9223372036854775807); -- Max BIGINT

-- Error case: Out-of-range negative inventory and sales
INSERT INTO f_inv_movmnt VALUES
('101', 'A1', '2024-01-01T00:00:00.000+0000', -100),
('102', 'B2', '2024-01-01T00:00:00.000+0000', -200);

INSERT INTO f_sales VALUES
('101', 'A1', '2024-01-01T00:00:00.000+0000', -10),
('102', 'B2', '2024-01-01T00:00:00.000+0000', -20);

-- NULL handling: Inventory and sales with NULL values
INSERT INTO f_inv_movmnt VALUES
('101', 'A1', '2024-01-01T00:00:00.000+0000', NULL);

INSERT INTO f_sales VALUES
('102', 'B2', '2024-01-01T00:00:00.000+0000', NULL);

-- Special characters and multi-byte characters in product and plant IDs
INSERT INTO f_inv_movmnt VALUES
('产品101', '工厂A1', '2024-01-01T00:00:00.000+0000', 400);

INSERT INTO f_sales VALUES
('产品101', '工厂A1', '2024-01-01T00:00:00.000+0000', 40);

-- Additional test records for diverse scenarios
INSERT INTO f_inv_movmnt VALUES
('103', 'C3', '2024-03-01T00:00:00.000+0000', 800),
('104', 'D4', '2024-03-02T00:00:00.000+0000', 300);

INSERT INTO f_sales VALUES
('103', 'C3', '2024-03-01T00:00:00.000+0000', 80),
('104', 'D4', '2024-03-02T00:00:00.000+0000', 30);

-- Ensure data security by restricting access to these tables
GRANT SELECT ON f_inv_movmnt TO someUserRole;
GRANT SELECT ON f_sales TO someUserRole;
