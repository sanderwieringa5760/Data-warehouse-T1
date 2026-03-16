ALTER TABLE ingestion.crm_prd_info
ADD cat_id VARCHAR(5);

ALTER TABLE transformation.crm_prd_info
ADD cat_id VARCHAR(5);

SELECT * FROM ingestion.crm_prd_info;

SELECT * FROM transformation.crm_prd_info;

SELECT * FROM transformation.crm_sales_details
WHERE sls_price IS NULL;

SELECT sls_quantity, count(*)
FROM transformation.crm_sales_details
WHERE sls_quantity > 1
GROUP BY sls_quantity

SELECT 
	sls_ord_num, sls_prd_key, COUNT(*) 
FROM 
	ingestion.crm_sales_details
GROUP BY 
	sls_ord_num, sls_prd_key
HAVING COUNT(*) > 1


SELECT * FROM
	ingestion.crm_sales_details 
WHERE 
	sls_ord_num = 'SO55367'

SELECT * FROM
	ingestion.crm_cust_info

select*
from ingestion.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_ship_dt > sls_due_dt

select*
from ingestion.crm_sales_details

select sls_ord_num, 
min(sls_order_dt) AS min_date,
max(sls_order_dt) AS max_date,
count(*)
from ingestion.crm_sales_details
group by sls_ord_num
having 
(count(*) > 1 and min(sls_order_dt) != max(sls_order_dt) )
or
(count(*) = 1 and min(sls_order_dt) = 0 )


select*
from ingestion.crm_sales_details
where sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * sls_price
or sls_price <= 0 or sls_price is null or sls_quantity <= 0 or sls_quantity is null



