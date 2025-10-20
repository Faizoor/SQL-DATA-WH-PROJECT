/*
================================================================================
Quality Checks
================================================================================
Description:
      This script performs various quality checks to ensure data consistency,
      accuracy, and standardization across the Silver schema. It includes:
        - Null or duplicate primary keys
        - Unwanted whitespace characters in string fields
        - Data standardization and consistency
        - Invalid date ranges
        - Data consistency between related fields

Notes:
      Run these checks after completing the ETL process.

PS:
      These checks are based on the available data — feel free to perform
      as many additional checks as you need.

================================================================================
*/

-- ================================================================================
-- Checking silver.crm_cust_info
-- ================================================================================
-- Checks
  -- Null & Duplicate Primary key 
  -- Invalid dates 
  - Data standarization and consistency 

select cst_key, count(*) from silver.crm_cust_info
group by cst_key
having count(*) > 1 or cst_key is null

select * from silver.crm_prd_info
where prd_end_dt < prd_start_dt

select distinct cst_gndr from silver.crm_cust_info

--===================================================================================
-- Checking silver.crm_prd_info
-- ==================================================================================

select prd_id, count(*) from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null

select prd_nm from silver.crm_prd_info
where prd_nm != trim(prd_nm)

-- =========================================================================================
-- Checking Silver.crm_sales_details
-- =========================================================================================

select sls_order_dt from Silver.crm_sales_details
where sls_order_dt > sls_due_dt

select sls_sales as old_sales,
sls_quantity,
sls_price as old_price,
CASE
	WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price) THEN sls_quantity * abs(sls_price)
	ELSE sls_sales
	END as sls_sales,
CASE
	WHEN sls_price is null or sls_price <=0 THEN sls_sales/nullif(sls_quantity ,0)
	ELSE sls_price
	END as sls_price
from 
Silver.crm_sales_details
where sls_sales is null or sls_sales <=0 or sls_quantity is null or sls_quantity <0
or sls_price is null or sls_price <=0 

-- ================================================================================================
-- Checking Silver.erp_cust_az12
-- ================================================================================================

select * from silver.erp_cust_az12
where bdate > GETDATE()

select distinct gen from silver.erp_cust_az12

-- =================================================================================================
-- Checking Silver.erp_loc_a101
-- =================================================================================================

select distinct cntry from silver.erp_loc_a101












