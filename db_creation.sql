-- This command creates a new database named TPCDS if it doesn't exist already. If it does exist, it replaces it with an empty database.
create or replace database TPCDS;
USE TPCDS;

create or replace schema RAW;

create or replace table TPCDS.RAW.inventory(
 inv_date_sk int NOT NULL, 
 inv_item_sk int NOT NULL, 
 inv_quantity_on_hand int, 
 inv_warehouse_sk int NOT NULL
);

-- Users are entities that can connect to a database and perform operations based on their assigned permissions.
create or replace user sami
password='';

grant role accountadmin to user sami;