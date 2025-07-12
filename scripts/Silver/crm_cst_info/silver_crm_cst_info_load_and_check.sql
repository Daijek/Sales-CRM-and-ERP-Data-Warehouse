INSERT INTO silver.crm_cust_info (
	[cst_id],
	[cst_key],
	[cst_firstname],
	[cst_lastname],
	[cst_marital_status],
	[cst_gndr],
	[cst_create_date])
	


SELECT 
	[cst_id],
	[cst_key],
	TRIM([cst_firstname]) AS [cst_firstname],
	TRIM([cst_lastname]) AS [cst_lastname],
	CASE 
		WHEN TRIM(UPPER([cst_marital_status])) = 'S' THEN 'Single'
		WHEN TRIM(UPPER([cst_marital_status])) = 'M' THEN 'Married'
		ELSE 'N/A'
	END [cst_marital_status],

	CASE
		WHEN TRIM(UPPER([cst_gndr])) = 'M' THEN 'Male'
		WHEN TRIM(UPPER([cst_gndr])) = 'F' THEN 'Female'
		ELSE 'N/A'
	END [cst_gndr],
	[cst_create_date]

FROM (
	SELECT *,
		-- ranking the cst_id column by create data
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM [DataWarehouse].[bronze].[crm_cust_info]
	WHERE cst_id IS NOT NULL
)t

WHERE flag_last = 1



