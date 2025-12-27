/*
=====================================================================================================
DDL Script: Create Bronze Tables
=====================================================================================================
Script Purpose:
  This script creates tables in the 'bronze' schema, dropping existing tables if they already exist.
  Run this script to re-define the DDL structure of 'bronze' Tables
=====================================================================================================
*/
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- CREATE DATABASE DataWarehouse
 
Use DataWarehouse;
GO

-- Create schemas

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
---
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info(
    cst_id INT
    ,cst_key NVARCHAR(50)
    ,cst_firstname NVARCHAR(50)
    ,cst_lastname NVARCHAR(50)
    ,cst_marital_status NVARCHAR(50)
    ,cst_gndr NVARCHAR(50)
    ,cst_create_date DATE
);

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
    prd_id INT
   ,prd_key NVARCHAR(50)
   ,prd_nm NVARCHAR(50)
   ,prd_cost INT
   ,prd_line NVARCHAR(50)
   ,prd_start_dt DATETIME
   ,prd_end_dt  DATETIME
)
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(
    sls_ord_num NVARCHAR(50)
    ,sls_prd_key NVARCHAR(50)
    ,sls_cust_id INT
    ,sls_order_dt INT
    ,sls_ship_dt INT
    ,sls_due_dt INT
    ,sls_sales INT
    ,sls_quantity INT
    ,sls_price INT
)
IF OBJECT_ID('bronze.erp_loc_A101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_A101;

CREATE TABLE bronze.erp_loc_A101(
    cid NVARCHAR(50)
    ,cntry NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
    cid NVARCHAR(50)
    ,bdate DATE
    ,gen NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2(
    id NVARCHAR(50)
    ,cat NVARCHAR(50)
    ,subcat NVARCHAR(50)
    ,maintenance  NVARCHAR(50)
);

--BULK INSERT
-- copy the file from the folder to docker path
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
