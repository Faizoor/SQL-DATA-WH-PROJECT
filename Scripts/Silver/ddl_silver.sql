/*
=============================================================================================================
DDL Script: Create Silver Tables
=============================================================================================================
Script Purpose:
	This Script creates tables in the Silver schema as a landing base, dropping 
	existing tables if they exists 
=============================================================================================================
*/

if object_id ('Silver.crm_cust_info', 'U') is NOT NULL 
	DROP table Silver.crm_cust_info;
GO 

create table Silver.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_satus NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE ,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


if object_id ('Silver.crm_prd_info', 'U') is NOT NULL 
	DROP table Silver.crm_prd_info;
GO 

create table Silver.crm_prd_info (
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost NVARCHAR(50),
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


if object_id ('Silver.crm_sales_details', 'U') is NOT NULL 
	DROP table Silver.crm_sales_details;
GO 


Create table Silver.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sld_prd_key NVARCHAR(50),
	sls_cust_id INT ,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


if object_id ('Silver.erp_cust_az12', 'U') is NOT NULL 
	DROP table Silver.erp_cust_az12;
GO 

create table Silver.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


if object_id ('Silver.erp_loc_a101', 'U') is NOT NULL 
	DROP table Silver.erp_loc_a101;
GO 

create table Silver.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO 

if object_id ('Silver.erp_px_cat_g1v2', 'U') is NOT NULL 
	DROP table Silver.erp_px_cat_g1v2;
GO

create table Silver.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
