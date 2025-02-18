-- This SQL query retrieves all data from the Delta table located in 
-- the Unity Catalog under the schema purgo_playground.purgo_playground
-- and the table name is f_inv_movmnt.
-- Use the appropriate Databricks Delta syntax and ensure that the
-- SQL statement utilizes best practices for readability and performance.

-- SQL query begins here
SELECT *
FROM purgo_playground.purgo_playground.f_inv_movmnt;
