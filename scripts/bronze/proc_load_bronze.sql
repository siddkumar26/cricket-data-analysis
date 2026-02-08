USE CricketWarehouse;
GO

/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
	BEGIN TRY
		SET @start_time = GETDATE();
		PRINT '======================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '======================================================';

		PRINT '>> Truncating Table: bronze.deliveries_updated_ipl_upto_2025';
		TRUNCATE TABLE bronze.deliveries_updated_ipl_upto_2025;

		PRINT '>> Inserting data to: bronze.deliveries_updated_ipl_upto_2025';
		BULK INSERT bronze.deliveries_updated_ipl_upto_2025
		FROM 'C:\SQL Project\cricket-data-analysis\Source\deliveries_updated_ipl_upto_2025.csv'
		WITH (
			FORMAT = 'CSV',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		PRINT '>> Truncating Table: bronze.matches_updated_ipl_upto_2025';
		TRUNCATE TABLE bronze.matches_updated_ipl_upto_2025;

		PRINT '>> Inserting data to: bronze.matches_updated_ipl_upto_2025';
		BULK INSERT bronze.matches_updated_ipl_upto_2025
		FROM 'C:\SQL Project\cricket-data-analysis\Source\matches_updated_ipl_upto_2025.csv'
		WITH (
			FORMAT = 'CSV',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '======================================================';
		PRINT 'Bronze Layer Loading completed!'
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +  ' seconds';
		PRINT '======================================================';
	END TRY
	BEGIN CATCH
		PRINT '======================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT '======================================================';
	END CATCH
END

EXEC bronze.load_bronze
