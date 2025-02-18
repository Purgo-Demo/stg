-- Drop the clone table if it exists for a fresh start
DROP TABLE IF EXISTS purgo_playground.t1_master_replica_clone;

-- Create a replica of the t1_master_replica table to t1_master_replica_clone
CREATE TABLE purgo_playground.t1_master_replica_clone AS
SELECT * FROM purgo_playground.t1_master_replica;

/* Update t1_query column for specific conditions */

-- For country_cd = 'NL' and sequence = 1
-- Update query to create a view with concatenated address fields
UPDATE purgo_playground.t1_master_replica_clone
SET t1_query = "CREATE OR REPLACE TEMP VIEW nl_wholesaler_insert_query AS 
                SELECT Wholesaler_File_Name, Source_System_Name, Source_Product_ID, Source_Product_Description, 
                Brand, Customer_ID, Customer_Name, Post_Code, Net_Sales, Gross_Sales, Packs, Units, Sales_Date, Volume, 
                Country_Code, Bill_to_Customer_ID, Currency_Code, delivery_file_name, 
                CONCAT(Address_Line_1, ' ', City, ' ', State, ' ', Country) AS Full_address 
                FROM purgo_playground.stg_nl_wholesaler"
WHERE country_cd = 'NL' AND sequence = 1;

-- For country_cd = 'NL' and sequence = 2
-- Update query to delete records
UPDATE purgo_playground.t1_master_replica_clone
SET t1_query = "DELETE FROM purgo_playground.stg_nl_wholesaler WHERE Country_Code = 'NL'"
WHERE country_cd = 'NL' AND sequence = 2;

-- For country_cd = 'NL' and sequence = 3
-- Update query to insert data from the view into r_t1_daily_sales
UPDATE purgo_playground.t1_master_replica_clone
SET t1_query = "INSERT INTO purgo_playground.r_t1_daily_sales SELECT * FROM nl_wholesaler_insert_query"
WHERE country_cd = 'NL' AND sequence = 3;

/* Validations to ensure updates */
-- Validate first update
SELECT t1_query
FROM purgo_playground.t1_master_replica_clone
WHERE country_cd = 'NL' AND sequence = 1;

-- Validate second update
SELECT t1_query
FROM purgo_playground.t1_master_replica_clone
WHERE country_cd = 'NL' AND sequence = 2;

-- Validate third update
SELECT t1_query
FROM purgo_playground.t1_master_replica_clone
WHERE country_cd = 'NL' AND sequence = 3;
