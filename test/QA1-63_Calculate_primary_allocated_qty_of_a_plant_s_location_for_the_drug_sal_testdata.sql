-- Generate comprehensive test data for `purgo_playground.f_order` ensuring diverse scenarios

-- Create table for storing test data
CREATE TABLE IF NOT EXISTS purgo_playground.f_order_test (
  order_nbr STRING,
  order_line_nbr STRING,
  primary_qty DOUBLE,
  open_qty DOUBLE,
  shipped_qty DOUBLE,
  cancel_qty DOUBLE,
  expected_allocated_qty DOUBLE
);

-- Insert test data
INSERT INTO purgo_playground.f_order_test (order_nbr, order_line_nbr, primary_qty, open_qty, shipped_qty, cancel_qty, expected_allocated_qty) VALUES
  -- Happy path test data
  ('O123', 'L001', 100.0, 50.0, 30.0, 20.0, 160.0), -- Standard allocation
  ('O456', 'L002', 200.0, 20.0, 10.0, 15.0, 215.0), -- Standard allocation

  -- Edge cases: boundary conditions
  ('O303', 'L003', 0.0, 0.0, 0.0, 0.0, 0.0),       -- All zero quantities
  ('O404', 'L004', 0.0, 0.0, 0.0, 10.0, -10.0),    -- Zero primary, open, shipped; positive cancel

  -- Error cases: invalid input scenarios
  ('O789', 'L005', 10.0, 5.0, 3.0, 20.0, NULL),    -- Negative result, should not calculate
  ('O101', 'L006', NULL, NULL, NULL, 5.0, NULL),  -- Missing primary, open, shipped quantities

  -- NULL handling scenarios
  ('O202', 'L007', NULL, 10.0, 20.0, 5.0, NULL),  -- NULL primary_qty
  ('O303', 'L008', 10.0, NULL, 20.0, 5.0, NULL),  -- NULL open_qty
  ('O404', 'L009', 10.0, 5.0, NULL, 5.0, NULL),   -- NULL shipped_qty

  -- Special characters and multi-byte characters
  ('OΩμTest', 'L010', 10.0, 5.0, 5.0, 2.0, 18.0), -- Multi-byte order_nbr

  -- Validate large number allocations
  ('O500', 'L011', 9999999999.99, 0.01, 0.0, 0.0, 10000000000.0), -- Large primary_qty

  -- Allocate precision testing
  ('OPrec', 'L012', 100.123456, 200.654321, 50.123456, 20.123456, 330.777777); -- Precision check

-- Query to test allocated_qty calculation according to defined formula
SELECT 
  order_nbr,
  order_line_nbr,
  primary_qty,
  open_qty,
  shipped_qty,
  cancel_qty,
  (primary_qty + open_qty + shipped_qty - cancel_qty) AS calculated_allocated_qty,
  expected_allocated_qty
FROM purgo_playground.f_order_test;
