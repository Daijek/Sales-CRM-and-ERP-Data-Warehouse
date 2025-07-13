-- Check Overall Quality
SELECT 
	[CID],
	[bdate],
	[gen]
FROM [DataWarehouse].[silver].[erp_cust_az12]

-- Checking for the different lengths of CID
SELECT 
	[CID]
FROM [DataWarehouse].[silver].[erp_cust_az12]
WHERE LEN(CID) != 13 AND LEN(CID) != 10

-- Checking for birth dates after the current date.
SELECT 
	[bdate]
FROM [DataWarehouse].[silver].[erp_cust_az12]
WHERE [bdate] > GETDATE()

-- Checking for the gender low cardinality column for consistency
SELECT DISTINCT
	[gen]
FROM [DataWarehouse].[silver].[erp_cust_az12]
