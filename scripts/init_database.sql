/*
====================================================================
Create Databas and Schemas
====================================================================

SCRIPT: This script creates a new databse named 'DataWarehouse' after checking if it already exists.  
        If the database exists, it is dropped and recreated.
        Additionally, the script sets up three schemas within the database: 'bronze', 'silver', 'gold'.

WARNING: Running this script will drop the entire Database 'DataWarehouse' if it exists.
         All data in the database will be permanently deleted. Proceed with caution and ensure
        you have proper backups before running this script
*/


USE master;
GO
--drop and recreate the 'DataWarehouse' database
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the database 'DataWarehouse' 
CREATE DATABASE DataWarehouse;
GO
  
Use DataWarehouse;
GO

-- Create schemas

CREATE SCHEMA bronze;
GO
  
CREATE SCHEMA silver;
GO
  
CREATE SCHEMA gold;
GO
--------------------------

