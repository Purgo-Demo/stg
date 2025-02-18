-- This SQL script calculates the allocated quantity for orders in the inventory management system
-- based on the purgo_playground schema and the f_order table.

-- Create a Delta table for allocated quantities derived from the f_order table
CREATE TABLE IF NOT EXISTS purgo_playground.allocated_order_summary
USING DELTA
PARTITIONED BY (order_nbr)
LOCATION '/mnt/delta/allocated_order_summary' 
AS
SELECT 
  order_nbr,
  order_line_nbr,
  primary_qty,
  open_qty,
  shipped_qty,
  cancel_qty,
  -- Compute allocated quantity using specified business logic
  (COALESCE(primary_qty, 0) + COALESCE(open_qty, 0) + COALESCE(shipped_qty, 0) - COALESCE(cancel_qty, 0)) AS allocated_qty,
  -- Track when this record was created/updated
  current_timestamp() AS last_updated
FROM 
  purgo_playground.f_order;

-- Optimize the newly created delta table by Z-ordering based on 'order_nbr'
-- to improve query performance, especially for reading order-specific data
-- Note: Z-Ordering should be part of an ALTER TABLE command or similar optimization command if present in Databricks
OPTIMIZE purgo_playground.allocated_order_summary
ZORDER BY (order_nbr);

-- Implement data validation: Ensure no negative 'allocated_qty' values
-- Log any negative values for further investigation
CREATE OR REPLACE TEMPORARY VIEW invalid_allocations AS
SELECT 
  order_nbr, 
  allocated_qty
FROM 
  purgo_playground.allocated_order_summary
WHERE 
  allocated_qty < 0;

-- Error handling: Capture and handle invalid data scenarios
-- If there are records with negative allocated_qty, log them for review
SELECT 
  order_nbr, 
  allocated_qty 
FROM 
  invalid_allocations;

-- Time Travel: Example usage of querying previous versions of the Delta table
-- Retrieve snapshot of the data as of a specific timestamp or version (commented for reference)
-- SELECT * FROM purgo_playground.allocated_order_summary TIMESTAMP AS OF '2023-01-01 00:00:00';

-- Perform routine housekeeping tasks on the Delta table by vacuuming
-- Removes old versions to save storage space, retains 7 days by default
VACUUM purgo_playground.allocated_order_summary
RETAIN 168 HOURS;

/* Documentation: Comments included to follow best practices for enforceability and readability
   Ensure resource cleanup and error handling, especially for temporary resources */
