/*

In this process of loading into the silver layer, the main goal is to clean, standardize, 
and transform the data coming from bronze, ensuring its quality and consistency for further 
analysis. Relationships between tables are implicitly validated, duplicates are removed 
using logic such as ROW_NUMBER(), and null or invalid values are corrected. Transformations 
like TRIM, REPLACE, and normalization of categorical fields (e.g., gender or marital status) 
are applied. In addition, business rules are enforced, such as correcting negative or 
inconsistent values in sales and prices, validating dates and their proper order, and 
performing data type casting. Derived columns are also generated (e.g., end dates using 
window functions), and the dataset is enriched to make it more useful for analysis, 
resulting in cleaner, structured, and business-aligned data.


*/


--EXEC silver.load_silver


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY 
    SET @batch_start_time = GETDATE();
        Print  '=================================================='
		PRINT  'Loading Silver layer'
		Print  '=================================================='

        Print  '--------------------------------------------------'
		PRINT  'Loading CRM Tables'
		Print  '--------------------------------------------------'


        SET @start_time = GETDATE();
        Print '>> Truncating table: silver.crm_cust_info';
        Truncate table silver.crm_cust_info
        PRINT '>> Inserting Data into: silver.crm_cust_info ';

        -- crm_cust_info
        Insert into silver.crm_cust_info(
	        cst_id,
	        cst_key,
	        cst_firstname,
	        cst_lastname,
	        cst_marital_status,
	        cst_gndr,
	        cst_create_date)
        select
        cst_id,
        cst_key,
        TRIM(cst_firstname) As cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
	         WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	         Else 'n/a'
        END cst_marital_status,
        CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
	         WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	         Else 'n/a'
        END cst_gndr,
        cst_create_date
        from(
        select 
        *,
        ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
        from bronze.crm_cust_info
        where cst_id is not null
        )t where flag_last = 1 
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time) AS nvarchar) + ' seconds';
        Print '>> --------------------------------------';



        SET @start_time = GETDATE();
        
        Print '>> Truncating table: silver.crm_prd_info';
        Truncate table silver.crm_prd_info
        PRINT '>> Inserting Data into: silver.crm_prd_info';
        -- crm_prd_info


        INSERT INTO silver.crm_prd_info(
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt      
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key,7, len(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost,0) AS prd_cost,
            CASE UPPER(TRIM(prd_line)) 
                 WHEN 'M' THEN 'Mountain'
                 WHEN 'R' THEN 'Road'
                 WHEN 'S' THEN 'Other sales'
                 WHEN 'T' THEN 'Touring'
                 Else 'n/a'
            END AS prd_line,
            CAST (prd_start_dt AS DATE) AS prd_start_dt,
            CAST (LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt) - 1 AS DATE) AS prd_end_dt
            FROM bronze.crm_prd_info
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time) AS nvarchar) + ' seconds';
        Print '>> --------------------------------------';



        SET @start_time = GETDATE();
        Print '>> Truncating table: silver.crm_sales_details';
        Truncate table silver.crm_sales_details
        PRINT '>> Inserting Data into: silver.crm_sales_details ';
        --crm_sales_details

        insert into silver.crm_sales_details(
          sls_ord_num,
	        sls_prd_key,
	        sls_cust_id,
	        sls_order_dt,
	        sls_ship_dt,
	        sls_due_dt,
	        sls_sales,
	        sls_quantity,
	        sls_price
        )
        select 
            sls_ord_num,     
            sls_prd_key,   
            sls_cust_id,  
            CASE 
                WHEN len(sls_order_dt)!=8 or sls_order_dt=0 THEN null 
                ELSE CAST(CAST(sls_order_dt AS Varchar) AS date)
            END AS sls_order_dt,
            CASE 
                WHEN len(sls_ship_dt)!=8 or sls_ship_dt=0 THEN null 
                ELSE CAST(CAST(sls_ship_dt AS Varchar) AS date)
            END AS sls_ship_dt,  
            CASE 
                WHEN len(sls_due_dt)!=8 or sls_due_dt=0 THEN null 
                ELSE CAST(CAST(sls_due_dt AS Varchar) AS date)
            END AS sls_due_dt,   
            CASE 
                WHEN sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales, 
            sls_quantity,   
            CASE 
                 WHEN sls_price <= 0 or sls_price is null 
                 THEN sls_sales / NULLIF(sls_quantity,0)
                 ELSE sls_price
            END AS sls_price
        from bronze.crm_sales_details;
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time) AS nvarchar) + ' seconds';


        Print  '--------------------------------------------------'
		PRINT  'Loading ERP Tables'
		Print  '--------------------------------------------------'


        SET @start_time = GETDATE();
        Print '>> Truncating table: silver.erp_cust_AZ12';
        Truncate table silver.erp_cust_AZ12
        PRINT '>> Inserting Data into: silver.erp_cust_AZ12';
        --erp_cust_AZ12
        insert into silver.erp_cust_AZ12(
            cid,
            bdate,
            gen
        )
        select distinct 
           CASE WHEN cid like 'NAS%' THEN SUBSTRING (cid,4,len(cid)) 
                ELSE cid
            END AS cid,     
            CASE WHEN bdate > GETDATE() THEN null
                ELSE bdate
            END AS bdate,  
            CASE WHEN UPPER(TRIM(gen)) in ('F','Female')  THEN 'Female'
                 WHEN UPPER(TRIM(gen)) in ('M','Male')  THEN 'Male'
            ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_AZ12
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time) AS nvarchar) + ' seconds';
        Print '>> --------------------------------------';



        SET @start_time = GETDATE();
        Print '>> Truncating table: silver.erp_LOC_A101';
        Truncate table silver.erp_LOC_A101
        PRINT '>> Inserting Data into: silver.erp_LOC_A101';
        --erp_LOC_A101
        insert into silver.erp_LOC_A101(
	        CID,
	        CNTRY
        )
        select 
            REPLACE(CID,'-','') as cid,
            CASE WHEN UPPER(TRIM(cntry)) = 'US' or UPPER(TRIM(cntry)) = 'USA' THEN 'United States'
                 WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                 WHEN TRIM(cntry)='' OR cntry is null then 'n/a'
                 ELSE cntry
            END AS cntry
        from bronze.erp_LOC_A101
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time) AS nvarchar) + ' seconds';
        Print '>> --------------------------------------';


        SET @start_time = GETDATE();
        Print '>> Truncating table: silver.erp_PX_CAT_G1V2';
        Truncate table silver.erp_PX_CAT_G1V2
        PRINT '>> Inserting Data into: silver.erp_PX_CAT_G1V2';
        --erp_PX_CAT_G1V2;
        insert into silver.erp_PX_CAT_G1V2
        (ID,CAT,SUBCAT,MAINTENANCE)
            select
            ID,
            CAT,
            SUBCAT,
            maintenance
        from bronze.erp_PX_CAT_G1V2
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time) AS nvarchar) + ' seconds';

    END TRY
    BEGIN CATCH
        PRINT '============================================'
		PRINT 'ERROR OCURRED DURING LOADING SILVER LAYER'
		PRINT 'ERROR Message: ' + ERROR_MESSAGE();
		PRINT 'ERROR Message: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR Message: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '============================================'
    END CATCH
END
