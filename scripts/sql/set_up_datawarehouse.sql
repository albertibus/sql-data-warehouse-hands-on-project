/*
=============================================================
Create Schemas
=============================================================
Script Purpose:
    Set up the data warehouse:
        -The script sets up three schemas within the database: 'bronze', 'silver', and 'gold'. If the
        schema exists it is dropped and recreated again.*/

DROP SCHEMA IF EXISTS bronze CASCADE;
CREATE SCHEMA bronze;

DROP SCHEMA IF EXISTS silver CASCADE;
CREATE SCHEMA silver;

DROP SCHEMA IF EXISTS gold CASCADE;
CREATE SCHEMA gold;