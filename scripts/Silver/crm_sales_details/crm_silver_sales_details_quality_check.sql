-- Check for NULL or duplicate values in product key
-- Expectation: No result
SELECT [sls_ord_num], [sls_prd_key], COUNT(*) AS 'ID Count'
FROM [DataWarehouse].[silver].[crm_sales_details]
GROUP BY [sls_ord_num], [sls_prd_key]
HAVING COUNT(*) > 1

-- Check for unwanted spaces in the string columns
SELECT [sls_ord_num]
FROM [DataWarehouse].[silver].[crm_sales_details]
WHERE TRIM([sls_ord_num]) != [sls_ord_num]

SELECT [sls_prd_key]
FROM [DataWarehouse].[silver].[crm_sales_details]
WHERE TRIM([sls_prd_key]) != [sls_prd_key]

-- Check for values equal to or less than 1 in the date columns
-- Expectation: No Result
--SELECT [sls_order_dt]
--FROM [DataWarehouse].[silver].[crm_sales_details]
--WHERE [sls_order_dt] <= 0

-- Check for date values that have a length less than 8
-- Expectation: No result
--SELECT [sls_order_dt]
--FROM [DataWarehouse].[silver].[crm_sales_details]
--WHERE [sls_order_dt] != 0

-- Validate the boundaries of date range (date range allowed by business)
-- Check invalid dates


-- Check for negative values, Nulls or Zeros in sales column
SELECT [sls_sales],[sls_quantity],[sls_price]
FROM [DataWarehouse].[silver].[crm_sales_details]
WHERE [sls_sales] IS NULL OR [sls_sales] <= 0

-- check for columns that dont have the sales as sales quantity * sales price
SELECT [sls_sales],[sls_quantity],[sls_price]
FROM [DataWarehouse].[silver].[crm_sales_details]
WHERE [sls_sales] != [sls_price] * [sls_quantity]

-- Check for price columns less than 0 or null
SELECT [sls_sales],[sls_quantity],[sls_price]
FROM [DataWarehouse].[silver].[crm_sales_details]
WHERE [sls_price] <= 0 OR [sls_price] IS NULL;

-- Check for quantity less than 0 or null
SELECT [sls_sales],[sls_quantity],[sls_price]
FROM [DataWarehouse].[silver].[crm_sales_details]
WHERE [sls_quantity] <= 0 OR [sls_quantity] IS NULL;

