/*
===============================================================================
Load Bronze Layer Script: Truncate 'Bronze' Tables
===============================================================================
Script Purpose:
    This script truncates tables in the Bronze layer.*/

TRUNCATE TABLE bronze.crm_customer_info;
TRUNCATE TABLE bronze.crm_prd_info;
TRUNCATE TABLE bronze.crm_sales_details;
TRUNCATE TABLE bronze.erp_cust_az12;
TRUNCATE TABLE bronze.erp_loc_a101;
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
