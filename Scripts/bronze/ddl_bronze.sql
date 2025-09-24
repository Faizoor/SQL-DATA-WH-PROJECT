/*
=============================================================================================================
DDL Script: Create Bronze Tables
=============================================================================================================
Script Purpose:
	This Script creates tables in the bronze schema as a landing base, dropping 
	existing tables if they exists 
=============================================================================================================
*/

if object_id ('bronze.crm_cust_info', 'U') is NOT NULL 
	DROP table bronze.crm_cust_info;
GO 

create table bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_satus NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE 
);
GO


if object_id ('bronze.crm_prd_info', 'U') is NOT NULL 
	DROP table bronze.crm_prd_info;
GO 

create table bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_num NVARCHAR(50),
	prd_cost NVARCHAR(50),
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME 
);
GO


if object_id ('bronze.crm_sales_details', 'U') is NOT NULL 
	DROP table bronze.crm_sales_details;
GO 


Create table bronze.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sld_prd_key NVARCHAR(50),
	sls_cust_id INT ,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
GO


if object_id ('bronze.erp_cust_az12', 'U') is NOT NULL 
	DROP table bronze.erp_cust_az12;
GO 

create table bronze.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);
GO


if object_id ('bronze.erp_loc_a101', 'U') is NOT NULL 
	DROP table bronze.erp_loc_a101;
GO 

create table bronze.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);
GO 

if object_id ('bronze.erp_px_cat_g1v2', 'U') is NOT NULL 
	DROP table bronze.erp_px_cat_g1v2;
GO

create table bronze.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);
GO

