-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No result

SELECT [prd_id], COUNT(*) AS [prd_id_count]
FROM [DataWarehouse].[silver].[crm_prd_info]
GROUP BY [prd_id]
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted spaces
-- Expectation: No result
SELECT [prd_nm]
FROM [DataWarehouse].[silver].[crm_prd_info]
WHERE TRIM([prd_nm]) != [prd_nm];

-- Check for Nulls and Negative Numbers
-- Expectation: No Results
SELECT [prd_cost]
FROM [DataWarehouse].[silver].[crm_prd_info]
WHERE [prd_cost] IS NULL OR [prd_cost] < 0;

-- Data Standardization and consistency
SELECT DISTINCT [prd_line]
FROM [DataWarehouse].[silver].[crm_prd_info]


--
SELECT *
FROM [DataWarehouse].[silver].[crm_prd_info]
WHERE [prd_end_dt] < [prd_start_dt]

SELECT * FROM [DataWarehouse].[silver].[crm_prd_info]