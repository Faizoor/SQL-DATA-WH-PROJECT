/*
====================================================================================
Stored Procedure: Load silver layer ( Bronze -> silver)
====================================================================================

*/

CREATE OR ALTER PROCEDURE SILVER.LOAD_SILVER AS 
BEGIN 
	DECLARE @START_TIME DATETIME , @BATCH_START_TIME DATETIME , @BATCH_END_TIME DATETIME ,@END_TIME DATETIME;
	BEGIN TRY 
		SET @BATCH_START_TIME= GETDATE();
		PRINT'===============================================================';
		PRINT'LOADING SILVER LAYER';
		PRINT'===============================================================';

		PRINT'----------------------------------------------------------------';
		PRINT'LODING CRM TABLES';
		PRINT'----------------------------------------------------------------';

		SET @START_TIME= GETDATE();

		PRINT'>>TRUNCATING CRM_CUST_INFO TABLE' ;
		TRUNCATE TABLE silver.crm_cust_info ;
		PRINT'>> LOADING CRM_CUST_INFO TABLE';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_satus,
			cst_gndr,
			cst_create_date
			)
		select 
		cst_id,
		cst_key,
		trim(cst_firstname),
		trim(cst_lastname),
		case 
			when upper(TRIM(cst_marital_satus)) ='M' then 'Married'
			when upper(TRIM(cst_marital_satus)) ='S' then 'Single'
			else 'N/A'
		end as cst_marital_satus,
		case 
			when upper(TRIM(cst_gndr)) ='M' then 'Male'
			when upper(TRIM(cst_gndr)) ='F' then 'Female'
			else 'N/A'
		end as cst_gndr,
		cst_create_date
		from (
			select 
			*,
			ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
			from 
			bronze.crm_cust_info 
			) as r
		where r.flag_last=1 ;

		SET @END_TIME = GETDATE();
		PRINT'LOAD DURATION '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT'--------------------------------------------------------------------------';


		SET @START_TIME=GETDATE();

		PRINT'>> TRUNCATE silver.crm_prd_info TABLE';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT'>> LOADING silver.crm_prd_info TABLE ';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)
		select 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
			SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
			prd_num,
			coalesce(prd_cost,0),
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring' 
				ELSE 'N/A'
			END as prd_line,
			CAST(prd_start_dt as DATE) as prd_start_dt,
			CAST(LEAD(prd_start_dt) over (PARTITION BY prd_key order by prd_start_dt)-1 as DATE) as prd_end_dt
		from 
		bronze.crm_prd_info;

		SET @END_TIME=GETDATE();
		PRINT'LOADING DURATIN '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT'---------------------------------------------------------------------------';


		SET @START_TIME=GETDATE();

		PRINT'>> TRUNCATE silver.crm_sales_details TABLE';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT'>> LOADING silver.crm_sales_details TABLE ';
		INSERT INTO Silver.crm_sales_details(
			sls_ord_num ,
			sld_prd_key ,
			sls_cust_id  ,
			sls_order_dt ,
			sls_ship_dt ,
			sls_due_dt ,
			sls_sales ,
			sls_quantity ,
			sls_price 
		)
		select 
		sls_ord_num,
		sld_prd_key,
		sls_cust_id,
			CASE WHEN len(sls_order_dt) != 8 or sls_order_dt =0 THEN NULL 
			ELSE CAST(CAST(sls_order_dt as VARCHAR) as DATE)
		END as sls_order_dt,
			CASE WHEN len(sls_ship_dt) != 8 or sls_ship_dt =0 THEN NULL 
			ELSE CAST(CAST(sls_ship_dt as VARCHAR) as DATE)
		END as sls_ship_dt,
			CASE WHEN len(sls_due_dt) != 8 or sls_due_dt =0 THEN NULL 
			ELSE CAST(CAST(sls_due_dt as VARCHAR) as DATE)
		END as sls_due_dt,
		CASE
			WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price) THEN sls_quantity * abs(sls_price)
			ELSE sls_sales
			END as sls_sales,
		sls_quantity,
		CASE
			WHEN sls_price is null or sls_price <=0 THEN sls_sales/nullif(sls_quantity ,0)
			ELSE sls_price
			END as sls_price
		from 
		bronze.crm_sales_details;

		SET @END_TIME=GETDATE();
		PRINT'LOADING DURATIN '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT'---------------------------------------------------------------------------';

		PRINT'----------------------------------------------------------------';
		PRINT'LODING ERP TABLES';
		PRINT'----------------------------------------------------------------';


		SET @START_TIME=GETDATE();

		PRINT'>> TRUNCATE silver.erp_cust_az12 TABLE';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT'>> LOADING silver.erp_cust_az12 TABLE ';
		INSERT INTO silver.erp_cust_az12 ( cid,bdate,gen)
		select 
		CASE 
			WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,len(cid))
			ELSE cid
		END as cid ,
		CASE
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END bdate,
		CASE 
			WHEN UPPER(TRIM(gen))in ('F','FEMALE') THEN 'FEMALE'
			WHEN UPPER(TRIM(gen)) in ('M', 'MALE') THEN 'MALE' 
			ELSE 'N/A'
		END as gen
		from 
		bronze.erp_cust_az12 ;

		SET @END_TIME=GETDATE();
		PRINT'LOADING DURATIN '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT'---------------------------------------------------------------------------';

		SET @START_TIME=GETDATE();

		PRINT'>> TRUNCATE silver.erp_loc_a101 TABLE';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT'>> LOADING silver.erp_loc_a101 TABLE ';
		INSERT INTO silver.erp_loc_a101 
		(cid,cntry)
		select 
		REPLACE(cid,'-','') as cid_new ,
		CASE 
			WHEN upper(TRIM(cntry))='DE' or upper(TRIM(cntry))='GERMANY' THEN 'GERMANY'  
			WHEN upper(TRIM(cntry))='USA' or upper(TRIM(cntry))='UNITED STATES' or upper(TRIM(cntry))='US' THEN 'UNITED STATES'
			WHEN upper(TRIM(cntry))='AUSTRALIA' or upper(TRIM(cntry))='AUS' THEN 'AUSTRALIA'
			WHEN upper(TRIM(cntry))='UNITED KINGDOM' or upper(TRIM(cntry))='UK' THEN 'UNITED KINGDOM'
			WHEN upper(TRIM(cntry))='CANADA' or upper(TRIM(cntry))='CA' THEN 'CANADA'
			WHEN upper(TRIM(cntry))='FRANCE' or upper(TRIM(cntry))='FR' THEN 'FRANCE'
			WHEN upper(TRIM(cntry))='' or cntry is NULL THEN 'N/A'
			ElSE upper(TRIM(cntry))
		END as cntry
		from bronze.erp_loc_a101 ;

		SET @END_TIME=GETDATE();
		PRINT'LOADING DURATIN '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT'---------------------------------------------------------------------------';

		SET @START_TIME=GETDATE();

		PRINT'>> TRUNCATE silver.erp_px_cat_g1v2 TABLE';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT'>> LOADING silver.erp_px_cat_g1v2 TABLE ';
		INSERT INTO silver.erp_px_cat_g1v2 
		(id,cat,subcat,maintenance)
		select 
			id,
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2;

		SET @END_TIME=GETDATE();
		PRINT'LOADING DURATIN '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT'---------------------------------------------------------------------------';
		SET @BATCH_END_TIME = GETDATE();
		PRINT 'Total Load Duration: '+ CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT'==================================================';

	END TRY 
	BEGIN CATCH 
		PRINT '==========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================';
	END CATCH 

END 
