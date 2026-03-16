SELECT * from
ingestion.crm_cust_info

SELECT * from
transformation.crm_cust_info

SELECT * from
transformation.crm_cust_info
TRUNCATE
table transformation.crm_cust_info



SELECT TOP (1000) 
	[prd_id],
	[prd_key],
	[prd_nm],
	[prd_cost],
	[prd_line],
	[prd_start_dt],
	[prd_end_dt],
	[cat_id]
FROM 
	transformation.crm_prd_info

SELECT 
	prd_id, count(*)
FROM 
	ingestion.crm_prd_info
GROUP BY 
	cat_id
HAVING 
	count(*) > 1 or prd_id is null

SELECT * FROM
	ingestion.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

SELECT * FROM ingestion.crm_prd_info
where prd_cost is null

SELECT * FROM
	ingestion.erp_px_cat_g1v2

SELECT * FROM
	transformation.crm_cust_info

ALTER TABLE
	transformation.crm_prd_info
ADD
	cat_id VARCHAR(5);

SELECT * FROM
	transformation.crm_prd_info

SELECT *
FROM
	ingestion.crm_sales_details
WHERE
	sls_quantity > 1;
	
SELECT *
FROM
	transformation.crm_sales_details
WHERE
	sls_quantity > 1;