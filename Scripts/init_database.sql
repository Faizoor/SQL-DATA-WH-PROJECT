/*

This script creates a new database named "datawarehouse" and within the database it will create theree schemas Bronze, silver and gold.

PS: if the database named "datawarehouse" already exists please drop and run the script 

*/

--Use System Database "Master"
use master ; 
Go

--Create Databse "datawarehouse"
create database datawarehouse ;
Go
use datawarehouse;
Go

--Create Schemas
create schema bronze;
Go
  
create schema silver;
Go

create schema gold;


