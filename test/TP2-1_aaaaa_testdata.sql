-- Create test data for f_inv_movmnt
CREATE OR REPLACE TABLE default.f_inv_movmnt (
  productId STRING,
  plantId STRING,
  inventory BIGINT,
  timestamp TIMESTAMP
);

INSERT INTO default.f_inv_movmnt VALUES
  -- Happy path scenario
  ('101', 'A1', 2000, TIMESTAMP('2024-01-15T12:00:00.000+0000')),
  ('101', 'A1', 1500, TIMESTAMP('2024-02-15T12:00:00.000+0000')),
  ('101', 'A1', 1800, TIMESTAMP('2024-03-15T12:00:00.000+0000')),
  
  -- Edge case: Boundary condition for zero inventory
  ('102', 'B2', 0, TIMESTAMP('2024-01-01T00:00:00.000+0000')),
  
  -- NULL handling scenario
  ('103', 'A1', NULL, TIMESTAMP('2024-03-20T10:00:00.000+0000')),
  
  -- Special characters
  ('104', '@!#$', 2500, TIMESTAMP('2024-02-10T09:00:00.000+0000'));

-- Create test data for f_sales
CREATE OR REPLACE TABLE default.f_sales (
  productId STRING,
  plantId STRING,
  sales DOUBLE,
  timestamp TIMESTAMP
);

INSERT INTO default.f_sales VALUES
  -- Happy path scenario
  ('101', 'A1', 300.5, TIMESTAMP('2024-01-15T12:00:00.000+0000')),
  ('101', 'A1', 350.75, TIMESTAMP('2024-02-15T12:00:00.000+0000')),
  ('101', 'A1', 400.0, TIMESTAMP('2024-03-15T12:00:00.000+0000')),
  
  -- Error case: Negative sales values
  ('102', 'B2', -150.0, TIMESTAMP('2024-01-01T00:00:00.000+0000')),
  
  -- NULL handling scenario
  ('103', 'A1', NULL, TIMESTAMP('2024-03-20T10:00:00.000+0000')),
  
  -- Multi-byte characters in plantId
  ('105', '厂区', 500.0, TIMESTAMP('2024-02-20T13:00:00.000+0000'));
