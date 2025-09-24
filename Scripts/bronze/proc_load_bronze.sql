/*
=============================================================================================================
Stored Procedure : bronze.load_bronze
=============================================================================================================
Description:
    This stored procedure truncates the target Bronze table and loads data from a CSV source 
    using BULK INSERT.

Purpose:
    - Ensure the Bronze layer always contains a fresh copy of the source data.
    - Maintain consistency by truncating the table before each load. 

Parameters:
    None

Execution:
  EXEC bronze.load_bronze;
=============================================================================================================
*/

CREATE or alter PROCEDURE bronze.load_bronze as 
BEGIN
	Declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY 
		SET @batch_start_time = GETDATE();
		PRINT '===========================================================';
		PRINT'Loading Bronze Layer ';
		PRINT '==========================================================='

		PRINT'------------------------------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating table: bronze.crm_cust_info';
		Truncate table bronze.crm_cust_info ;
		PRINT'>> Inserting Data into table: bronze.crm_cust_info';
		Bulk insert bronze.crm_cust_info 
		from 'C:\Users\Dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with 
			(
				FIRSTROW  = 2 ,
				FIELDTERMINATOR = ',',
				TABLOCK 
			)
		;
		SET @end_time=GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT'---------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating table: bronze.crm_prd_info';
		Truncate table bronze.crm_prd_info ;
		PRINT'>> Inserting Data into table: bronze.crm_prd_info';
		Bulk insert bronze.crm_prd_info
		from 'C:\Users\Dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with 
			(
				FIRSTROW  = 2 ,
				FIELDTERMINATOR = ',',
				TABLOCK 
			)
		;
		SET @end_time=GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT'---------------------------------';

		SET @start_time = GETDATE();
		PRINT('>> Truncating table: crm_sales_details');
		Truncate table bronze.crm_sales_details ;
		PRINT('>> Inserting Data into table: bronze.crm_sales_details');
		Bulk insert bronze.crm_sales_details
		from 'C:\Users\Dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with 
			(
				FIRSTROW  = 2 ,
				FIELDTERMINATOR = ',',
				TABLOCK 
			)
		;
		SET @end_time=GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT'>>---------------------------------';


		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating table: erp_cust_az12';
		Truncate table bronze.erp_cust_az12 ;
		PRINT'>> Inserting Data into table: bronze.erp_cust_az12';
		Bulk insert bronze.erp_cust_az12 
		from 'C:\Users\Dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with 
			(
				FIRSTROW  = 2 ,
				FIELDTERMINATOR = ',',
				TABLOCK 
			)
		;
		SET @end_time=GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT'>>---------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating table: erp_loc_a101';
		Truncate table bronze.erp_loc_a101 ;
		PRINT'>> Inserting Data into table: bronze.erp_loc_a101';
		Bulk insert bronze.erp_loc_a101
		from 'C:\Users\Dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with 
			(
				FIRSTROW  = 2 ,
				FIELDTERMINATOR = ',',
				TABLOCK 
			)
		;
		SET @end_time=GETDATE();
		PRINT ('Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds');
		PRINT('>>---------------------------------');

		SET @start_time = GETDATE();
		PRINT'>> Truncating table: erp_px_cat_g1v2';
		Truncate table bronze.erp_px_cat_g1v2 ;
		PRINT'>> Inserting Data into table: bronze.erp_px_cat_g1v2';
		Bulk insert bronze.erp_px_cat_g1v2 
		from 'C:\Users\Dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with 
			(
				FIRSTROW  = 2 ,
				FIELDTERMINATOR = ',',
				TABLOCK 
			)
		;
		SET @end_time=GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT'>>---------------------------------';

		SET @batch_end_time= GETDATE();
		PRINT'==================================================';
		PRINT'Loading Bronze layer is completed';
		PRINT'==================================================';
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
