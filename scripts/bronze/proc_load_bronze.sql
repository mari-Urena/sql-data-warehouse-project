/*
Procedure: bronze.load_bronze

Description:
This stored procedure performs a full load of the Bronze layer.
It truncates existing tables and bulk loads ('bulk insert') raw data from CRM and ERP
CSV source files into the Data Warehouse.

Purpose:
Refresh the Bronze layer with the latest source data and log load durations.
*/



create or alter procedure bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	
	BEGIN TRY
	SET @batch_start_time = GETDATE();
			Print  '=================================================='
			PRINT  'Loading Bronze layer'
			Print  '=================================================='

	

			Print  '--------------------------------------------------'
			PRINT  'Loading CRM Tables'
			Print  '--------------------------------------------------'

			SET @start_time = GETDATE();
			PRINT '>> Truncating table: bronze.crm_cust_info '
			Truncate table bronze.crm_cust_info

			PRINT '>> Inserting data into: bronze.crm_cust_info'
			Bulk insert bronze.crm_cust_info
			From 'C:\data_engineer\sql\data_warehouse_project\datasets\source_crm\cust_info.csv'
			WITH(
				Firstrow = 2,
				fieldterminator = ',',
				Tablock
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time) AS nvarchar) + ' seconds';



			SET @start_time = GETDATE();
			PRINT '>> Truncating table: bronze.crm_prd_info '
			Truncate table bronze.crm_prd_info

			PRINT '>> Inserting data into: bronze.crm_prd_info'
			Bulk insert bronze.crm_prd_info
			From 'C:\data_engineer\sql\data_warehouse_project\datasets\source_crm\prd_info.csv'
			WITH(
				Firstrow = 2,
				fieldterminator = ',',
				Tablock
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time) AS nvarchar) + ' seconds';


			SET @start_time = GETDATE();
			PRINT '>> Truncating table: bronze.crm_sales_details'
			Truncate table bronze.crm_sales_details

			PRINT '>> Inserting data into: bronze.crm_sales_details'
			Bulk insert bronze.crm_sales_details
			From 'C:\data_engineer\sql\data_warehouse_project\datasets\source_crm\sales_details.csv'
			WITH(
				Firstrow = 2,
				fieldterminator = ',',
				Tablock
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time) AS nvarchar) + ' seconds';

	

			Print  '--------------------------------------------------'
			PRINT  'Loading ERP Tables'
			Print  '--------------------------------------------------'

	
			SET @start_time = GETDATE();
			PRINT '>> Truncating table: bronze.erp_cust_AZ12'
			Truncate table bronze.erp_cust_AZ12

			PRINT '>> Inserting data into: bronze.erp_cust_AZ12'
			Bulk insert bronze.erp_cust_AZ12
			From 'C:\data_engineer\sql\data_warehouse_project\datasets\source_erp\cust_AZ12.csv'
			WITH(
				Firstrow = 2,
				fieldterminator = ',',
				Tablock
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time) AS nvarchar) + ' seconds';


			
			SET @start_time = GETDATE();
			PRINT '>> Truncating table: bronze.erp_LOC_A101'
			Truncate table bronze.erp_LOC_A101

			PRINT '>> Inserting data into: bronze.erp_LOC_A101'
			Bulk insert bronze.erp_LOC_A101
			From 'C:\data_engineer\sql\data_warehouse_project\datasets\source_erp\LOC_A101.csv'
			WITH(
				Firstrow = 2,
				fieldterminator = ',',
				Tablock
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time) AS nvarchar) + ' seconds';


			
			SET @start_time = GETDATE();
			PRINT '>> Truncating table: bronze.erp_PX_CAT_G1V2'
			Truncate table bronze.erp_PX_CAT_G1V2

			PRINT '>> Inserting data into: bronze.erp_PX_CAT_G1V2'
			Bulk insert bronze.erp_PX_CAT_G1V2
			From 'C:\data_engineer\sql\data_warehouse_project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH(
				Firstrow = 2,
				fieldterminator = ',',
				Tablock
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time) AS nvarchar) + ' seconds';


			SET @batch_end_time = GETDATE();
			PRINT '=========================================='
			PRINT 'Loading Bronze Layer is Completed';
			PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
			PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '============================================'
		PRINT 'ERROR OCURRED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR Message: ' + ERROR_MESSAGE();
		PRINT 'ERROR Message: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR Message: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '============================================'
	END CATCH
END

