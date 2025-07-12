-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT 
	cst_id, 
	COUNT(*) AS cst_id_count
FROM [DataWarehouse].[silver].[crm_cust_info]
GROUP BY [cst_id]
HAVING COUNT(*) > 1 OR cst_id IS NULL;

--Check for unwanted spaces
-- Expectation: No Result
SELECT *
FROM [DataWarehouse].[silver].[crm_cust_info]
WHERE [cst_firstname] != TRIM(cst_firstname);

SELECT *
FROM [DataWarehouse].[silver].[crm_cust_info]
WHERE [cst_lastname] != TRIM(cst_lastname);

-- Check that low cardinality columns are standardized
SELECT DISTINCT [cst_gndr]
FROM [DataWarehouse].[silver].[crm_cust_info];

SELECT DISTINCT [cst_marital_status]
FROM [DataWarehouse].[silver].[crm_cust_info];