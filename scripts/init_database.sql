/*
============================================
Create Database and Schemas
============================================
Script Purpose:
  This script creates a new database named 'datawarehouse' after checking if it already exists.
  If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
  within the database: 'bronze', 'silver', and 'gold'.

Warning:
  Running this script will drop the entire 'dataware' database if it exits.
  All data in the database will be permanently deleted. Proceed with caution
  and ensure you have proper backups before running this script.
*/




Use master;
go


If exists (select 1 from sys.databases where name= 'datawarehouse')
Begin
	Alter database datawarehouse set single_user with rollback immediate;
	drop database datawarehouse;
end;
go


create database datawarehouse;
go

use datawarehouse;
go

create schema bronze;
go

create schema silver;
go

create schema gold;
go
