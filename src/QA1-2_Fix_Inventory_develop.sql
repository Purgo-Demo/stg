-- Add "lastdate" column to employees table with default NULL and validate it cannot be in future
ALTER TABLE purgo_playground.employees
ADD COLUMN lastdate DATE;

-- Set default values and add validations
CREATE OR REPLACE VIEW purgo_playground.valid_employees AS
SELECT *,
  CASE
    WHEN lastdate > CURRENT_DATE THEN NULL
    ELSE lastdate
  END AS lastdate
FROM purgo_playground.employees;

-- Backfill existing records with NULL
MERGE INTO purgo_playground.employees AS target
USING (
  SELECT employee_id
  FROM purgo_playground.valid_employees
  WHERE lastdate IS NULL
) AS source
ON target.employee_id = source.employee_id
WHEN MATCHED THEN
UPDATE SET target.lastdate = NULL;

-- Add "categoryGroup" column to customers table with default 'Uncategorized'
ALTER TABLE purgo_playground.customers
ADD COLUMN categoryGroup STRING DEFAULT 'Uncategorized';

-- Enforce length validation
CREATE OR REPLACE VIEW purgo_playground.valid_customers AS
SELECT *,
  CASE
    WHEN LENGTH(categoryGroup) <= 50 THEN categoryGroup
    ELSE 'Uncategorized'
  END AS categoryGroup
FROM purgo_playground.customers;

-- Backfill existing records with default value
MERGE INTO purgo_playground.customers AS target
USING (
  SELECT customer_id
  FROM purgo_playground.valid_customers
  WHERE categoryGroup = 'Uncategorized'
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
UPDATE SET target.categoryGroup = 'Uncategorized';
