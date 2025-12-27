/*
=====================================================================================================
Stored Procedure: Load Bronze Layer (Source-> Bronze)
=====================================================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
   - Truncate the bronze tables before loading data.
   - Uses the 'BULK INSERT' command to load data from csv files to bronze tables.
Parameters:
None.
This stored procedure doesnot accept any parameters or return any values.
Usage:
EXEC bronze.load_bronze;
=====================================================================================================
*/
-- **BULK INSERT**
  
-- copy the file from the folder to docker path
-- full path for self reference
--((base) LA@macbook-500 source_crm % docker cp cust_info.csv azuresqledge:/)
--docker cp cust_info.csv azuresqledge:/

-- create procedure

Use DataWarehouse
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME , @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
    
        SET @batch_start_time = GETDATE();
        PRINT'Loading Bronze layer';
        PRINT'=====================';
        PRINT'Loading CRM Tables';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM '/cust_info.csv'
        WITH
        (
            FORMAT = 'CSV',
            FIRSTROW=2

        );
        SET @end_time = GETDATE()
        PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        SET @start_time = GETDATE()
        TRUNCATE TABLE bronze.crm_prd_info

        BULK INSERT bronze.crm_prd_info
        FROM '/prd_info.csv'
        WITH
        (
            FORMAT = 'CSV',
            FIRSTROW=2
        )
        SET @end_time = GETDATE()
        PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        SET @start_time = GETDATE()
    
        TRUNCATE TABLE bronze.crm_sales_details
        BULK INSERT bronze.crm_sales_details
        FROM '/sales_details.csv'
        WITH
        (
            FORMAT = 'CSV',
            FIRSTROW=2
        )
        SET @end_time = GETDATE()
        PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT'Loading ERP DATA';
        SET @start_time = GETDATE()
        TRUNCATE TABLE bronze.erp_cust_az12
        BULK INSERT bronze.erp_cust_az12
        FROM '/cust_az12.csv'
        WITH
        (
            FORMAT = 'CSV',
            FIRSTROW=2
        )
        SET @end_time = GETDATE()
        PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        SET @start_time = GETDATE()
        TRUNCATE TABLE bronze.erp_loc_A101
        BULK INSERT bronze.erp_loc_A101
        FROM '/loc_a101.csv'
        WITH
        (
            FORMAT = 'CSV',
            FIRSTROW=2
        )
        SET @end_time = GETDATE()
        PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        SET @start_time = GETDATE()
        TRUNCATE TABLE bronze.erp_px_cat_g1v2
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/px_cat_g1v2.csv'
        WITH
        (
            FORMAT = 'CSV',
            FIRSTROW=2
        )
        SET @end_time = GETDATE()
        PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        SET @batch_end_time = GETDATE();
        PRINT'Loading BRONZE LAYER IS COMPLETED';
        PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT'==================================='
    END TRY
    BEGIN CATCH
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'ERROR Message' + ERROR_MESSAGE();
        PRINT 'ERROR Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
    END CATCH

END
