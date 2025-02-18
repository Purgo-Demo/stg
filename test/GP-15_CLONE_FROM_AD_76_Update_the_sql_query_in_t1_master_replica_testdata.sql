-- Step 1: Drop the {{t1_master_replica_clone}} table if it exists.
DROP TABLE IF EXISTS purgo_playground.t1_master_replica_clone;

-- Step 2: Create a replica of the {{t1_master_replica}} table.
CREATE TABLE purgo_playground.t1_master_replica_clone AS
SELECT * FROM purgo_playground.t1_master_replica;

-- Step 3: Update the {{t1_query}} column for sequence 1.
UPDATE purgo_playground.t1_master_replica_clone
SET t1_query = 'CREATE OR REPLACE TEMP VIEW nl_wholesaler_insert_query AS
  SELECT Wholesaler_File_Name, Source_System_Name, Source_Product_ID, Source_Product_Description, Brand, Customer_ID, Customer_Name, Post_Code, Net_Sales, Gross_Sales, Packs, Units, Sales_Date, Volume, Country_Code, Bill_to_Customer_ID, Currency_Code, delivery_file_name, 
  CONCAT(Address_Line_1, '' '', City, '' '', State, '' '', Country) AS Full_address 
  FROM purgo_playground.stg_nl_wholesaler'
WHERE country_cd = 'NL' AND sequence = 1;

-- Step 4: Update the {{t1_query}} column for sequence 2.
UPDATE purgo_playground.t1_master_replica_clone
SET t1_query = 'DELETE FROM purgo_playground.stg_nl_wholesaler WHERE Country_Code = ''NL'''
WHERE country_cd = 'NL' AND sequence = 2;

-- Step 5: Update the {{t1_query}} column for sequence 3.
UPDATE purgo_playground.t1_master_replica_clone
SET t1_query = 'INSERT INTO purgo_playground.r_t1_daily_sales SELECT * FROM nl_wholesaler_insert_query'
WHERE country_cd = 'NL' AND sequence = 3;
