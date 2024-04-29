--------------------------------------- 1. Business Requirements ---------------------------------------

--------------------- sum_qty_wk: The sum of sales_quantity for this week. ---------------------
-- sum(catalog_sales.cs_quantity) group by date_dim.week_num and item, OR sum(web_sales.ws_quantity) group by date_dim.week_num and item
    -- --> Grain: Week Number and Item

---------- CATALOG_SALES -----------
SELECT sum(CATALOG_SALES.CS_QUANTITY) as sum_qty_wk, CATALOG_SALES.CS_ITEM_SK , DATE_DIM.WK_NUM,
FROM CATALOG_SALES, DATE_DIM
WHERE CATALOG_SALES.CS_SOLD_DATE_SK = DATE_DIM.D_DATE_SK
GROUP BY DATE_DIM.WK_NUM, CATALOG_SALES.CS_ITEM_SK;

---------- WEB_SALES -----------
SELECT sum(WEB_SALES.WS_QUANTITY) as sum_qty_wk, WEB_SALES.WS_ITEM_SK, DATE_DIM.WK_NUM,
FROM WEB_SALES, DATE_DIM
WHERE WEB_SALES.WS_SOLD_DATE_SK = DATE_DIM.D_DATE_SK
GROUP BY DATE_DIM.WK_NUM, WEB_SALES.WS_ITEM_SK;

    
---------------------  sum_amt_wk: The sum of sales_amount for this week. ---------------------   
-- sum(catalog_sales.cs_sales_price * catalog_sales.cs_quantity) group by date_dim.week_num, item OR sum(web_sales.ws_sales_price * web_sales.ws_quantity) group by date_dim.week_num, item

--- catalog_sales
SELECT 
    SUM(cs.cs_sales_price * cs.cs_quantity) AS sum_amt_wk, 
    cs.cs_item_sk AS item_sk, 
    dd.wk_num AS week_num
FROM 
    catalog_sales cs
JOIN 
    date_dim dd ON cs.cs_sold_date_sk = dd.d_date_sk
GROUP BY 
    dd.wk_num, 
    cs.cs_item_sk;


--- web_sales

--------------------- sum_profit_wk: The sum of net_profit for this week.  ---------------------
-- sum(catalog_sales.cs_net_profit) group by date_dim.week_num, item OR sum(web_sales.ws_net_profit) group by date_dim.week_num, item

SELECT 
    SUM(cs.cs_net_profit) AS sum_profit_wk, 
    cs.cs_item_sk AS item_sk, 
    dd.wk_num AS week_num
FROM 
    catalog_sales cs
JOIN 
    date_dim dd ON cs.cs_sold_date_sk = dd.d_date_sk
GROUP BY 
    dd.wk_num, 
    cs.cs_item_sk;


--------------------- avg_qty_dy: The average daily sales_quantity for this week ---------------------
-- avg_qty_dy: = sum_qty_wk/7

SELECT 
    cs.cs_item_sk AS item_sk,
    dd.wk_num AS week_num,
    SUM(cs.cs_quantity)/7 AS avg_qty_dy
FROM 
    catalog_sales cs
JOIN 
    date_dim dd ON cs.cs_sold_date_sk = dd.d_date_sk
GROUP BY 
    dd.wk_num, 
    cs.cs_item_sk;



--------------------- inv_on_hand_qty_wk ---------------------
-- inv_on_hand_qty_wk: inventory.inv_quantity_on_hand at date_dim.week_num, warehouse

SELECT 
    inv.INV_ITEM_SK AS item_sk,
    inv.INV_WAREHOUSE_SK AS warehouse_sk,
    dd.WK_NUM AS week_num,
    SUM(inv.INV_QUANTITY_ON_HAND) AS inv_on_hand_qty_wk
FROM 
    inventory inv
JOIN 
    date_dim dd ON inv.INV_DATE_SK = dd.D_DATE_SK  -- Joining on date surrogate key
GROUP BY 
    dd.WK_NUM, 
    inv.INV_WAREHOUSE_SK,
    inv.INV_ITEM_SK;


--------------------- wks_sply: Weeks of Supply ---------------------
-- wks_sply: = inv_on_hand_qty_wk/sum_qty_wk

WITH InventorySales AS (
    -- Query to calculate inventory quantity on hand at the end of each week for each warehouse and item
    SELECT 
        inv.INV_ITEM_SK AS item_sk,
        inv.INV_WAREHOUSE_SK AS warehouse_sk,
        dd.WK_NUM AS week_num,
        SUM(inv.INV_QUANTITY_ON_HAND) AS inv_on_hand_qty_wk
    FROM 
        inventory inv
    JOIN 
        date_dim dd ON inv.INV_DATE_SK = dd.D_DATE_SK
    GROUP BY 
        dd.WK_NUM, 
        inv.INV_WAREHOUSE_SK,
        inv.INV_ITEM_SK
),
Sales AS (
    -- Query to calculate the sum of sales quantity for each week for each item
    SELECT 
        cs.CS_ITEM_SK AS item_sk,
        dd.WK_NUM AS week_num,
        SUM(cs.CS_QUANTITY) AS sum_qty_wk
    FROM 
        catalog_sales cs
    JOIN 
        date_dim dd ON cs.CS_SOLD_DATE_SK = dd.D_DATE_SK
    GROUP BY 
        dd.WK_NUM, 
        cs.CS_ITEM_SK
    UNION ALL
    SELECT 
        ws.WS_ITEM_SK AS item_sk,
        dd.WK_NUM AS week_num,
        SUM(ws.WS_QUANTITY) AS sum_qty_wk
    FROM 
        web_sales ws
    JOIN 
        date_dim dd ON ws.WS_SOLD_DATE_SK = dd.D_DATE_SK
    GROUP BY 
        dd.WK_NUM, 
        ws.WS_ITEM_SK
)
-- Calculate weeks of supply by joining the inventory and sales data
SELECT 
    COALESCE(inv.week_num, s.week_num) AS week_num,
    COALESCE(inv.item_sk, s.item_sk) AS item_sk,
    COALESCE(inv.warehouse_sk, 0) AS warehouse_sk,  -- Assuming warehouse_sk is 0 for total
    CASE 
        WHEN s.sum_qty_wk <> 0 THEN inv.inv_on_hand_qty_wk / s.sum_qty_wk
        ELSE 0  -- To avoid division by zero error
    END AS wks_sply
FROM 
    Sales s
LEFT JOIN 
    InventorySales inv ON s.item_sk = inv.item_sk AND s.week_num = inv.week_num;


--------------------- low_stock_flg_wk: Low stock weekly flag. ---------------------
-- For example, if there is a single day where (avg_qty_dy > 0 && (avg_qty_dy > inventory_on_hand_qty_wk)) in the week, then mark this week's flag as True. 
-- Integrate Customer Dimension: Customer(SCD Type 2) + Customer_Address + Customer_Demographics + Household_Demographics + Income_Band

WITH InventorySales AS (
    -- Query to calculate inventory quantity on hand at the end of each week for each warehouse and item
    SELECT 
        inv.INV_ITEM_SK AS item_sk,
        inv.INV_WAREHOUSE_SK AS warehouse_sk,
        dd.WK_NUM AS week_num,
        SUM(inv.INV_QUANTITY_ON_HAND) AS inv_on_hand_qty_wk
    FROM 
        inventory inv
    JOIN 
        date_dim dd ON inv.INV_DATE_SK = dd.D_DATE_SK
    GROUP BY 
        dd.WK_NUM, 
        inv.INV_WAREHOUSE_SK,
        inv.INV_ITEM_SK
),
Sales AS (
    -- Query to calculate the sum of sales quantity for each week for each item
    SELECT 
        cs.CS_ITEM_SK AS item_sk,
        dd.WK_NUM AS week_num,
        AVG(cs.CS_QUANTITY) AS avg_qty_dy
    FROM 
        catalog_sales cs
    JOIN 
        date_dim dd ON cs.CS_SOLD_DATE_SK = dd.D_DATE_SK
    GROUP BY 
        dd.WK_NUM, 
        cs.CS_ITEM_SK
    UNION ALL
    SELECT 
        ws.WS_ITEM_SK AS item_sk,
        dd.WK_NUM AS week_num,
        AVG(ws.WS_QUANTITY) AS avg_qty_dy
    FROM 
        web_sales ws
    JOIN 
        date_dim dd ON ws.WS_SOLD_DATE_SK = dd.D_DATE_SK
    GROUP BY 
        dd.WK_NUM, 
        ws.WS_ITEM_SK
),
-- Joining Sales and Inventory data
SalesInventory AS (
    SELECT 
        s.item_sk,
        s.week_num,
        s.avg_qty_dy,
        inv.inv_on_hand_qty_wk
    FROM 
        Sales s
    LEFT JOIN 
        InventorySales inv ON s.item_sk = inv.item_sk AND s.week_num = inv.week_num
)
-- Calculate low stock flag for each week
SELECT 
    s.item_sk,
    s.week_num,
    CASE 
        WHEN MAX(s.avg_qty_dy) > 0 AND MAX(s.avg_qty_dy) > MAX(s.inv_on_hand_qty_wk) THEN 'True'
        ELSE 'False'
    END AS low_stock_flg_wk
FROM 
    SalesInventory s
GROUP BY 
    s.item_sk, 
    s.week_num;


    
------ ## NEW Business Requirements ------

-- Customer lifetime value
SELECT 
    c.C_CUSTOMER_SK AS customer_id,
    SUM(COALESCE(ws.WS_NET_PAID, 0)) - SUM(COALESCE(ws.WS_WHOLESALE_COST, 0)) AS customer_lifetime_value
FROM 
    CUSTOMER c
JOIN 
    web_sales ws ON c.C_CUSTOMER_SK = ws.WS_BILL_CUSTOMER_SK
WHERE
    ws.WS_NET_PAID IS NOT NULL  -- Ensure non-null values for net paid amount
    AND ws.WS_WHOLESALE_COST IS NOT NULL -- Ensure non-null values for wholesale cost
GROUP BY 
    c.C_CUSTOMER_SK;


CREATE TABLE TPCDS.ANALYTICS.Customer_Lifetime_Value AS
SELECT 
    c.C_CUSTOMER_SK AS customer_id,
    -- by subtracting the total wholesale cost from the total net paid amount The COALESCE function is used to handle possible NULL values in the WS_NET_PAID and WS_WHOLESALE_COST columns by replacing them with 0.
    SUM(COALESCE(ws.WS_NET_PAID, 0)) - SUM(COALESCE(ws.WS_WHOLESALE_COST, 0)) AS customer_lifetime_value
FROM 
    CUSTOMER c
JOIN 
    web_sales ws ON c.C_CUSTOMER_SK = ws.WS_BILL_CUSTOMER_SK
WHERE
    ws.WS_NET_PAID IS NOT NULL  -- Ensure non-null values for net paid amount
    AND ws.WS_WHOLESALE_COST IS NOT NULL -- Ensure non-null values for wholesale cost
GROUP BY 
    c.C_CUSTOMER_SK;


----- Marketing Attribution 
SELECT 
    p.P_PROMO_NAME,
    COUNT(DISTINCT ws.WS_ORDER_NUMBER) AS attributed_sales
FROM 
    PROMOTION p
JOIN 
    web_sales ws ON p.P_PROMO_SK = ws.WS_PROMO_SK
GROUP BY 
    p.P_PROMO_NAME;

-- The purpose of this table is to store data related to marketing attribution, specifically the count of sales attributed to each promotion.
CREATE TABLE TPCDS.ANALYTICS.Marketing_Attribution AS
SELECT 
    p.P_PROMO_NAME,
    COUNT(DISTINCT ws.WS_ORDER_NUMBER) AS attributed_sales
FROM 
    PROMOTION p
JOIN 
    web_sales ws ON p.P_PROMO_SK = ws.WS_PROMO_SK
GROUP BY 
    p.P_PROMO_NAME;

---------------------------------------

------------------- Seasonal Sales Analysis by Month and Year -------------------
SELECT 
    EXTRACT(YEAR FROM dd.CAL_DT) AS year,
    EXTRACT(MONTH FROM dd.CAL_DT) AS month,
    SUM(ws.WS_NET_PAID) AS total_sales
FROM 
    web_sales ws
JOIN 
    date_dim dd ON ws.WS_SOLD_DATE_SK = dd.D_DATE_SK
GROUP BY 
    EXTRACT(YEAR FROM dd.CAL_DT),
    EXTRACT(MONTH FROM dd.CAL_DT)
ORDER BY 
    year, month;
    --------------------------------------------------------------------------

    
-- Calculate monthly sales
SELECT
    TO_CHAR(DATEADD(month, dd.d_date_sk, '1970-01-01'), 'YYYY-MM') AS month,
    SUM(ws.ws_sales_price) AS total_sales
FROM
    TPCDS.raw.date_dim AS dd
JOIN
    TPCDS.raw.web_sales AS ws ON dd.d_date_sk = ws.ws_sold_date_sk
GROUP BY
    month
ORDER BY
    month;

-- Calculate quarterly sales
SELECT
    CONCAT(EXTRACT(YEAR FROM DATEADD(month, dd.d_date_sk, '1970-01-01')), ' Q', (EXTRACT(MONTH FROM DATEADD(month, dd.d_date_sk, '1970-01-01'))-1)/3 + 1) AS quarter,
    SUM(ws.ws_sales_price) AS total_sales
FROM
    TPCDS.raw.date_dim AS dd
JOIN
    TPCDS.raw.web_sales AS ws ON dd.d_date_sk = ws.ws_sold_date_sk
GROUP BY
    quarter
ORDER BY
    quarter;

---  two new tables named Seasonal_Sales_Monthly and Seasonal_Sales_Quarterly within the TPCDS.ANALYTICS schema. These tables are intended to store sales data aggregated by month and quarter, respectively, for analyzing seasonal sales patterns.

CREATE TABLE TPCDS.ANALYTICS.Seasonal_Sales_Monthly AS
SELECT
    TO_CHAR(DATEADD(month, dd.d_date_sk, '1970-01-01'), 'YYYY-MM') AS month,
    SUM(ws.ws_sales_price) AS total_sales
FROM
    TPCDS.raw.date_dim AS dd
JOIN
    TPCDS.raw.web_sales AS ws ON dd.d_date_sk = ws.ws_sold_date_sk
GROUP BY
    month
ORDER BY
    month;
    
CREATE TABLE TPCDS.ANALYTICS.Seasonal_Sales_Quarterly AS
SELECT
    CONCAT(EXTRACT(YEAR FROM DATEADD(month, dd.d_date_sk, '1970-01-01')), ' Q', (EXTRACT(MONTH FROM DATEADD(month, dd.d_date_sk, '1970-01-01'))-1)/3 + 1) AS quarter,
    SUM(ws.ws_sales_price) AS total_sales
FROM
    TPCDS.raw.date_dim AS dd
JOIN
    TPCDS.raw.web_sales AS ws ON dd.d_date_sk = ws.ws_sold_date_sk
GROUP BY
    quarter
ORDER BY
    quarter;

--------------------------------------- Shipping Time Analysis -------------------------------------
SELECT 
    EXTRACT(YEAR FROM dd.CAL_DT) AS year,
    EXTRACT(MONTH FROM dd.CAL_DT) AS month,
    AVG(DATEDIFF(ws.WS_SHIP_DATE_SK, ws.WS_SOLD_DATE_SK)) AS avg_shipping_time
FROM 
    web_sales ws
JOIN 
    date_dim dd ON ws.WS_SOLD_DATE_SK = dd.D_DATE_SK
GROUP BY 
    EXTRACT(YEAR FROM dd.CAL_DT),
    EXTRACT(MONTH FROM dd.CAL_DT)
ORDER BY TPCDS.ANALYTICS.WAREHOUSE_DIM_DIM
    year, month;

------------------------------ shipping_time -------------------------------------
CREATE or replace TABLE TPCDS.ANALYTICS.shipping_time (
    shipment_id INT PRIMARY KEY,
    order_id INT,
    shipping_date NUMBER(38,0),
    delivery_date NUMBER(38,0),
    shipping_method VARCHAR(50),
    shipping_address VARCHAR(255)
);

-- inserts shipping-related data from the web_sales, warehouse, customer_address, and web_site tables into the shipping_time table. It extracts information such as shipment ID, order ID, shipping date, delivery date, shipping method, and shipping address, ensuring that the necessary data is present and correctly formatted before insertion.

INSERT INTO TPCDS.ANALYTICS.shipping_time (shipment_id, order_id, shipping_date, delivery_date, shipping_method, shipping_address)
SELECT
    ws.ws_ship_date_sk AS shipment_id,
    ws.ws_order_number AS order_id,
    ws.ws_ship_date_sk AS shipping_date,
    ws.ws_ship_date_sk AS delivery_date,
    CASE ws.ws_ship_mode_sk 
        WHEN 1 THEN 'Standard' 
        WHEN 2 THEN 'Air' 
        WHEN 3 THEN 'Truck' 
        WHEN 4 THEN 'Mail' 
        ELSE 'Other' 
    END AS shipping_method,
    CONCAT(web.web_street_name, ' ', web.web_city, ', ', web.web_state, ' ', web.web_zip) AS shipping_address -- This concatenates the street name, city, state, and ZIP code from the web_site table to form a complete shipping address.
    
FROM
    TPCDS.raw.web_sales AS ws
JOIN
    TPCDS.raw.warehouse AS wh ON ws.ws_warehouse_sk = wh.w_warehouse_sk
JOIN
    TPCDS.raw.customer_address AS ca ON ws.ws_ship_addr_sk = ca.ca_address_sk
JOIN
    TPCDS.raw.web_site AS web ON ws.ws_web_site_sk = web.web_site_sk
WHERE
    ws.ws_ship_date_sk IS NOT NULL
    AND ws.ws_order_number IS NOT NULL
    AND ws.ws_ship_mode_sk IS NOT NULL
    AND ws.ws_sold_date_sk IS NOT NULL;


-------------------------------------------------------------------

  
--------------------------------------- 2. Data Model ---------------------------------------
-- DDL

-- As Customer Snapshot is not ready, we will create it in a separate schema
CREATE OR REPLACE SCHEMA INTERMEDIATE; 

-- Creating Customer Snapshot Table
CREATE OR REPLACE TABLE TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT (
	C_SALUTATION VARCHAR(16777216),
	C_PREFERRED_CUST_FLAG VARCHAR(16777216),
	C_FIRST_SALES_DATE_SK NUMBER(38,0),
	C_CUSTOMER_SK NUMBER(38,0),
	C_LOGIN VARCHAR(16777216),
	C_CURRENT_CDEMO_SK NUMBER(38,0),
	C_FIRST_NAME VARCHAR(16777216),
	C_CURRENT_HDEMO_SK NUMBER(38,0),
	C_CURRENT_ADDR_SK NUMBER(38,0),
	C_LAST_NAME VARCHAR(16777216),
	C_CUSTOMER_ID VARCHAR(16777216),
	C_LAST_REVIEW_DATE_SK NUMBER(38,0),
	C_BIRTH_MONTH NUMBER(38,0),
	C_BIRTH_COUNTRY VARCHAR(16777216),
	C_BIRTH_YEAR NUMBER(38,0),
	C_BIRTH_DAY NUMBER(38,0),
	C_EMAIL_ADDRESS VARCHAR(16777216),
	C_FIRST_SHIPTO_DATE_SK NUMBER(38,0),
	START_DATE TIMESTAMP_NTZ(9),
	END_DATE TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE SCHEMA ANALYTICS;


-- Final Customer_Dim
create or replace TABLE TPCDS.ANALYTICS.CUSTOMER_DIM (
	C_SALUTATION VARCHAR(16777216),
	C_PREFERRED_CUST_FLAG VARCHAR(16777216),
	C_FIRST_SALES_DATE_SK NUMBER(38,0),
	C_CUSTOMER_SK NUMBER(38,0),
	C_LOGIN VARCHAR(16777216),
	C_CURRENT_CDEMO_SK NUMBER(38,0),
	C_FIRST_NAME VARCHAR(16777216),
	C_CURRENT_HDEMO_SK NUMBER(38,0),
	C_CURRENT_ADDR_SK NUMBER(38,0),
	C_LAST_NAME VARCHAR(16777216),
	C_CUSTOMER_ID VARCHAR(16777216),
	C_LAST_REVIEW_DATE_SK NUMBER(38,0),
	C_BIRTH_MONTH NUMBER(38,0),
	C_BIRTH_COUNTRY VARCHAR(16777216),
	C_BIRTH_YEAR NUMBER(38,0),
	C_BIRTH_DAY NUMBER(38,0),
	C_EMAIL_ADDRESS VARCHAR(16777216),
	C_FIRST_SHIPTO_DATE_SK NUMBER(38,0),
	CA_STREET_NAME VARCHAR(16777216),
	CA_SUITE_NUMBER VARCHAR(16777216),
	CA_STATE VARCHAR(16777216),
	CA_LOCATION_TYPE VARCHAR(16777216),
	CA_COUNTRY VARCHAR(16777216),
	CA_ADDRESS_ID VARCHAR(16777216),
	CA_COUNTY VARCHAR(16777216),
	CA_STREET_NUMBER VARCHAR(16777216),
	CA_ZIP VARCHAR(16777216),
	CA_CITY VARCHAR(16777216),
	CA_GMT_OFFSET FLOAT,
	CD_DEP_EMPLOYED_COUNT NUMBER(38,0),
	CD_DEP_COUNT NUMBER(38,0),
	CD_CREDIT_RATING VARCHAR(16777216),
	CD_EDUCATION_STATUS VARCHAR(16777216),
	CD_PURCHASE_ESTIMATE NUMBER(38,0),
	CD_MARITAL_STATUS VARCHAR(16777216),
	CD_DEP_COLLEGE_COUNT NUMBER(38,0),
	CD_GENDER VARCHAR(16777216),
	HD_BUY_POTENTIAL VARCHAR(16777216),
	HD_DEP_COUNT NUMBER(38,0),
	HD_VEHICLE_COUNT NUMBER(38,0),
	HD_INCOME_BAND_SK NUMBER(38,0),
	IB_LOWER_BOUND NUMBER(38,0),
	IB_UPPER_BOUND NUMBER(38,0),
	START_DATE TIMESTAMP_NTZ(9),
	END_DATE TIMESTAMP_NTZ(9)
);


create or replace TABLE TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY (
    WAREHOUSE_SK NUMBER(38,0),
	ITEM_SK NUMBER(38,0),
	SOLD_WK_SK NUMBER(38,0),
	SOLD_WK_NUM NUMBER(38,0),
	SOLD_YR_NUM NUMBER(38,0),
	SUM_QTY_WK NUMBER(38,0),
	SUM_AMT_WK FLOAT,
	SUM_PROFIT_WK FLOAT,
	AVG_QTY_DY NUMBER(38,6),
	INV_QTY_WK NUMBER(38,0),
	WKS_SPLY NUMBER(38,6),
	LOW_STOCK_FLG_WK BOOLEAN
);

create or replace TABLE TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES (
	WAREHOUSE_SK NUMBER(38,0),
	ITEM_SK NUMBER(38,0),
    SOLD_DATE_SK NUMBER(38,0),
    SOLD_WK_NUM NUMBER(38,0),
    SOLD_YR_NUM NUMBER(38,0),
	DAILY_QTY NUMBER(38,0),
	DAILY_SALES_AMT FLOAT,
	DAILY_NET_PROFIT FLOAT
);

------------------------- daily aggregated sales table --------------------------
-- Develop a merge script to incorporate the newly added daily sales records into the existing daily sales fact table within the Analytics schema. 

-- Handle the incremental update of daily sales data by first deleting any partial records from the last date in the existing table and then aggregating and inserting the newly added daily sales records. This ensures that the daily aggregated sales table remains up-to-date with the latest sales data.


SELECT * FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES;


-- This line retrieves the last sold date from the existing daily aggregated sales records and assigns it to the variable LAST_SOLD_DATE_SK.
SET LAST_SOLD_DATE_SK = (SELECT MAX(SOLD_DATE_SK) FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES);


-- deletes any partial records from the last date in the existing daily aggregated sales data. This ensures that any existing records for the last date are removed before inserting new records.
DELETE FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES WHERE sold_date_sk=$LAST_SOLD_DATE_SK;



CREATE OR REPLACE TEMPORARY TABLE TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES_TMP AS (
-- compiling all incremental sales records
with incremental_sales as (
SELECT 
            CS_WAREHOUSE_SK as warehouse_sk,
            CS_ITEM_SK as item_sk,
            CS_SOLD_DATE_SK as sold_date_sk,
            CS_QUANTITY as quantity,
            cs_sales_price * cs_quantity as sales_amt,
            CS_NET_PROFIT as net_profit
    from TPCDS.RAW.catalog_sales
    WHERE sold_date_sk >= NVL($LAST_SOLD_DATE_SK,0) 
        and quantity is not null
        and sales_amt is not null
    
    union all

    SELECT 
            WS_WAREHOUSE_SK as warehouse_sk,
            WS_ITEM_SK as item_sk,
            WS_SOLD_DATE_SK as sold_date_sk,
            WS_QUANTITY as quantity,
            ws_sales_price * ws_quantity as sales_amt,
            WS_NET_PROFIT as net_profit
    from TPCDS.RAW.web_sales
    WHERE sold_date_sk >= NVL($LAST_SOLD_DATE_SK,0) 
        and quantity is not null
        and sales_amt is not null
),

aggregating_records_to_daily_sales as
(
select 
    warehouse_sk,
    item_sk,
    sold_date_sk, 
    sum(quantity) as daily_qty,
    sum(sales_amt) as daily_sales_amt,
    sum(net_profit) as daily_net_profit 
from incremental_sales
group by 1, 2, 3

),

adding_week_number_and_yr_number as
(
select 
    *,
    date.wk_num as sold_wk_num,
    date.yr_num as sold_yr_num
from aggregating_records_to_daily_sales 
LEFT JOIN TPCDS.RAW.date_dim date 
    ON sold_date_sk = d_date_sk

)

SELECT 
	warehouse_sk,
    item_sk,
    sold_date_sk,
    max(sold_wk_num) as sold_wk_num,
    max(sold_yr_num) as sold_yr_num,
    sum(daily_qty) as daily_qty,
    sum(daily_sales_amt) as daily_sales_amt,
    sum(daily_net_profit) as daily_net_profit 
FROM adding_week_number_and_yr_number
GROUP BY 1,2,3
ORDER BY 1,2,3
);

-- Inserting new records
INSERT INTO TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES
(	
    WAREHOUSE_SK, 
    ITEM_SK, 
    SOLD_DATE_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    DAILY_QTY, 
    DAILY_SALES_AMT, 
    DAILY_NET_PROFIT
)
SELECT 
    DISTINCT -- The DISTINCT statement is used to return only distinct (different) values.
	warehouse_sk,
    item_sk,
    sold_date_sk,
    sold_wk_num,
    sold_yr_num,
    daily_qty,
    daily_sales_amt,
    daily_net_profit 
FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES_TMP;

------------ Weekly Sales Inventory Fact Table ----------

-- Develop a merge script to incorporate the newly added daily sales records into the existing daily sales fact table within the Analytics schema. 

-- Handle the incremental update of daily sales data by first deleting any partial records from the last date in the existing table and then aggregating and inserting the newly added daily sales records. This ensures that the daily aggregated sales table remains up-to-date with the latest sales data.


SELECT * FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES;


-- This line retrieves the last sold date from the existing daily aggregated sales records and assigns it to the variable LAST_SOLD_DATE_SK.
SET LAST_SOLD_DATE_SK = (SELECT MAX(SOLD_DATE_SK) FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES);


-- deletes any partial records from the last date in the existing daily aggregated sales data. This ensures that any existing records for the last date are removed before inserting new records.
DELETE FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES WHERE sold_date_sk=$LAST_SOLD_DATE_SK;



CREATE OR REPLACE TEMPORARY TABLE TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES_TMP AS (
-- compiling all incremental sales records
with incremental_sales as (
SELECT 
            CS_WAREHOUSE_SK as warehouse_sk,
            CS_ITEM_SK as item_sk,
            CS_SOLD_DATE_SK as sold_date_sk,
            CS_QUANTITY as quantity,
            cs_sales_price * cs_quantity as sales_amt,
            CS_NET_PROFIT as net_profit
    from TPCDS.RAW.catalog_sales
    WHERE sold_date_sk >= NVL($LAST_SOLD_DATE_SK,0) 
        and quantity is not null
        and sales_amt is not null
    
    union all

    SELECT 
            WS_WAREHOUSE_SK as warehouse_sk,
            WS_ITEM_SK as item_sk,
            WS_SOLD_DATE_SK as sold_date_sk,
            WS_QUANTITY as quantity,
            ws_sales_price * ws_quantity as sales_amt,
            WS_NET_PROFIT as net_profit
    from TPCDS.RAW.web_sales
    WHERE sold_date_sk >= NVL($LAST_SOLD_DATE_SK,0) 
        and quantity is not null
        and sales_amt is not null
),

aggregating_records_to_daily_sales as
(
select 
    warehouse_sk,
    item_sk,
    sold_date_sk, 
    sum(quantity) as daily_qty,
    sum(sales_amt) as daily_sales_amt,
    sum(net_profit) as daily_net_profit 
from incremental_sales
group by 1, 2, 3

),

adding_week_number_and_yr_number as
(
select 
    *,
    date.wk_num as sold_wk_num,
    date.yr_num as sold_yr_num
from aggregating_records_to_daily_sales 
LEFT JOIN TPCDS.RAW.date_dim date 
    ON sold_date_sk = d_date_sk

)

SELECT 
	warehouse_sk,
    item_sk,
    sold_date_sk,
    max(sold_wk_num) as sold_wk_num,
    max(sold_yr_num) as sold_yr_num,
    sum(daily_qty) as daily_qty,
    sum(daily_sales_amt) as daily_sales_amt,
    sum(daily_net_profit) as daily_net_profit 
FROM adding_week_number_and_yr_number
GROUP BY 1,2,3
ORDER BY 1,2,3
);

-- Inserting new records
INSERT INTO TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES
(	
    WAREHOUSE_SK, 
    ITEM_SK, 
    SOLD_DATE_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    DAILY_QTY, 
    DAILY_SALES_AMT, 
    DAILY_NET_PROFIT
)
SELECT 
    DISTINCT -- The DISTINCT statement is used to return only distinct (different) values.
	warehouse_sk,
    item_sk,
    sold_date_sk,
    sold_wk_num,
    sold_yr_num,
    daily_qty,
    daily_sales_amt,
    daily_net_profit 
FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES_TMP;
-- Dimensions: week_num, item, warehouse
-- Facts: sum_qty_wk, sum_amt_wk, sum_profit_wk, avg_qty_dy, inv_on_hand_qty_wk, wks_sply,low_stock_flg_wk
-- This SQL script simplifies the process of merging new weekly sales data into the current inventory. It begins by identifying the latest recorded week, clearing any incomplete records from that timeframe, and consolidating daily sales figures into weekly summaries. Following this, the summarized data is formatted for easy integration into the inventory table. 

-- Getting Last Date
SET LAST_SOLD_WK_SK = (SELECT MAX(SOLD_WK_SK) FROM TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY);

-- Removing partial records from the last date
DELETE FROM TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY WHERE sold_wk_sk=$LAST_SOLD_WK_SK;

-- compiling all incremental sales records
CREATE OR REPLACE TEMPORARY TABLE TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY_TMP AS (
with aggregating_daily_sales_to_week as (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK, 
    MIN(SOLD_DATE_SK) AS SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM(DAILY_QTY) AS SUM_QTY_WK, 
    SUM(DAILY_SALES_AMT) AS SUM_AMT_WK, 
    SUM(DAILY_NET_PROFIT) AS SUM_PROFIT_WK
FROM
    TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES
GROUP BY
    1,2,4,5
HAVING 
    sold_wk_sk >= NVL($LAST_SOLD_WK_SK,0)
),

-- We need to have the same sold_wk_sk for all the items. Currently, any items that didn't have any sales on Sunday (first day of the week) would not have Sunday date as sold_wk_sk so this CTE will correct that.
finding_first_date_of_the_week as (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK, 
    date.d_date_sk AS SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_QTY_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK
FROM
    aggregating_daily_sales_to_week daily_sales
INNER JOIN TPCDS.RAW.DATE_DIM as date
on daily_sales.SOLD_WK_NUM=date.wk_num
and daily_sales.sold_yr_num=date.yr_num
and date.day_of_wk_num=0
),

-- This will help sales and inventory tables to join together using wk_num and yr_num
date_columns_in_inventory_table as (
SELECT 
    inventory.*,
    date.wk_num as inv_wk_num,
    date.yr_num as inv_yr_num
FROM
    tpcds.RAW.inventory inventory
INNER JOIN TPCDS.RAW.DATE_DIM as date
on inventory.inv_date_sk = date.d_date_sk
)

select 
       warehouse_sk, 
       item_sk, 
       min(SOLD_WK_SK) as sold_wk_sk,
       sold_wk_num as sold_wk_num,
       sold_yr_num as sold_yr_num,
       sum(sum_qty_wk) as sum_qty_wk,
       sum(sum_amt_wk) as sum_amt_wk,
       sum(sum_profit_wk) as sum_profit_wk,
       sum(sum_qty_wk)/7 as avg_qty_dy,
       sum(coalesce(inv.inv_quantity_on_hand, 0)) as inv_qty_wk, 
       sum(coalesce(inv.inv_quantity_on_hand, 0)) / sum(sum_qty_wk) as wks_sply,
       iff(avg_qty_dy>0 and avg_qty_dy>inv_qty_wk, true , false) as low_stock_flg_wk
from finding_first_date_of_the_week
left join date_columns_in_inventory_table inv 
    on inv_wk_num = sold_wk_num and inv_yr_num = sold_yr_num and item_sk = inv_item_sk and inv_warehouse_sk = warehouse_sk
group by 1, 2, 4, 5
-- extra precaution because we don't want negative or zero quantities in our final model
having sum(sum_qty_wk) > 0
);

-- Inserting new records
INSERT INTO TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY
(	
	WAREHOUSE_SK, 
    ITEM_SK, 
    SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_QTY_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK, 
    AVG_QTY_DY, 
    INV_QTY_WK, 
    WKS_SPLY, 
    LOW_STOCK_FLG_WK
    
)
SELECT 
    DISTINCT -- The DISTINCT statement is used to return only distinct (different) values.
	WAREHOUSE_SK, 
    ITEM_SK, 
    SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_QTY_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK, 
    AVG_QTY_DY, 
    INV_QTY_WK, 
    WKS_SPLY, 
    LOW_STOCK_FLG_WK
FROM TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY_TMP;

select * from TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY;


------------ Customer Dimension ----------
-- Develop a merge script to integrate the new Customer dimension table into the existing dimension table within the Analytics schema, following Type 2 methodology. 

SELECT * FROM TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT;

-- merge script compares the records from the existing snapshot (t1) with the new customer data from the raw table (t2) based on specified matching conditions. 

-- If a match is not found (using the "WHEN NOT MATCHED" clause), it inserts the new record into the snapshot table. 
MERGE INTO TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT t1
USING TPCDS.RAW.CUSTOMER t2
ON  t1.C_SALUTATION=t2.C_SALUTATION
    AND t1.C_PREFERRED_CUST_FLAG=t2.C_PREFERRED_CUST_FLAG 
    AND coalesce(t1.C_FIRST_SALES_DATE_SK, 0) = coalesce(t2.C_FIRST_SALES_DATE_SK,0) 
    AND t1.C_CUSTOMER_SK=t2.C_CUSTOMER_SK
    AND t1.C_LOGIN=t2.C_LOGIN
    AND coalesce(t1.C_CURRENT_CDEMO_SK,0) = coalesce(t2.C_CURRENT_CDEMO_SK,0)
    AND t1.C_FIRST_NAME=t2.C_FIRST_NAME
    AND coalesce(t1.C_CURRENT_HDEMO_SK,0) = coalesce(t2.C_CURRENT_HDEMO_SK,0)
    AND t1.C_CURRENT_ADDR_SK=t2.C_CURRENT_ADDR_SK
    AND t1.C_LAST_NAME=t2.C_LAST_NAME
    AND t1.C_CUSTOMER_ID=t2.C_CUSTOMER_ID
    AND coalesce(t1.C_LAST_REVIEW_DATE_SK,0) = coalesce(t2.C_LAST_REVIEW_DATE_SK,0)
    AND coalesce(t1.C_BIRTH_MONTH,0) = coalesce(t2.C_BIRTH_MONTH,0)
    AND t1.C_BIRTH_COUNTRY = t2.C_BIRTH_COUNTRY
    AND coalesce(t1.C_BIRTH_YEAR,0) = coalesce(t2.C_BIRTH_YEAR,0)
    AND coalesce(t1.C_BIRTH_DAY,0) = coalesce(t2.C_BIRTH_DAY,0)
    AND t1.C_EMAIL_ADDRESS = t2.C_EMAIL_ADDRESS
    AND coalesce(t1.C_FIRST_SHIPTO_DATE_SK,0) = coalesce(t2.C_FIRST_SHIPTO_DATE_SK,0)
WHEN NOT MATCHED 
THEN INSERT (
    C_SALUTATION, 
    C_PREFERRED_CUST_FLAG, 
    C_FIRST_SALES_DATE_SK, 
    C_CUSTOMER_SK, C_LOGIN, 
    C_CURRENT_CDEMO_SK, 
    C_FIRST_NAME, 
    C_CURRENT_HDEMO_SK, 
    C_CURRENT_ADDR_SK, 
    C_LAST_NAME, 
    C_CUSTOMER_ID, 
    C_LAST_REVIEW_DATE_SK, 
    C_BIRTH_MONTH, 
    C_BIRTH_COUNTRY, 
    C_BIRTH_YEAR, 
    C_BIRTH_DAY, 
    C_EMAIL_ADDRESS, 
    C_FIRST_SHIPTO_DATE_SK,
    START_DATE,
    END_DATE)
VALUES (
    t2.C_SALUTATION, 
    t2.C_PREFERRED_CUST_FLAG, 
    t2.C_FIRST_SALES_DATE_SK, 
    t2.C_CUSTOMER_SK, 
    t2.C_LOGIN, 
    t2.C_CURRENT_CDEMO_SK, 
    t2.C_FIRST_NAME, 
    t2.C_CURRENT_HDEMO_SK, 
    t2.C_CURRENT_ADDR_SK, 
    t2.C_LAST_NAME, 
    t2.C_CUSTOMER_ID, 
    t2.C_LAST_REVIEW_DATE_SK, 
    t2.C_BIRTH_MONTH, 
    t2.C_BIRTH_COUNTRY, 
    t2.C_BIRTH_YEAR, 
    t2.C_BIRTH_DAY, 
    t2.C_EMAIL_ADDRESS, 
    t2.C_FIRST_SHIPTO_DATE_SK,
    CURRENT_DATE(),
    NULL
);

-- If a match is found (using the "WHEN MATCHED" clause), it updates the end_date of the existing record in the snapshot table.
MERGE INTO TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT t1
USING TPCDS.RAW.CUSTOMER t2
ON  t1.C_CUSTOMER_SK=t2.C_CUSTOMER_SK
WHEN MATCHED
    AND (
    t1.C_SALUTATION!=t2.C_SALUTATION
    OR t1.C_PREFERRED_CUST_FLAG!=t2.C_PREFERRED_CUST_FLAG 
    OR coalesce(t1.C_FIRST_SALES_DATE_SK, 0) != coalesce(t2.C_FIRST_SALES_DATE_SK,0) 
    OR t1.C_LOGIN!=t2.C_LOGIN
    OR coalesce(t1.C_CURRENT_CDEMO_SK,0) != coalesce(t2.C_CURRENT_CDEMO_SK,0)
    OR t1.C_FIRST_NAME!=t2.C_FIRST_NAME
    OR coalesce(t1.C_CURRENT_HDEMO_SK,0) != coalesce(t2.C_CURRENT_HDEMO_SK,0)
    OR t1.C_CURRENT_ADDR_SK!=t2.C_CURRENT_ADDR_SK
    OR t1.C_LAST_NAME!=t2.C_LAST_NAME
    OR t1.C_CUSTOMER_ID!=t2.C_CUSTOMER_ID
    OR coalesce(t1.C_LAST_REVIEW_DATE_SK,0) != coalesce(t2.C_LAST_REVIEW_DATE_SK,0)
    OR coalesce(t1.C_BIRTH_MONTH,0) != coalesce(t2.C_BIRTH_MONTH,0)
    OR t1.C_BIRTH_COUNTRY != t2.C_BIRTH_COUNTRY
    OR coalesce(t1.C_BIRTH_YEAR,0) != coalesce(t2.C_BIRTH_YEAR,0)
    OR coalesce(t1.C_BIRTH_DAY,0) != coalesce(t2.C_BIRTH_DAY,0)
    OR t1.C_EMAIL_ADDRESS != t2.C_EMAIL_ADDRESS
    OR coalesce(t1.C_FIRST_SHIPTO_DATE_SK,0) != coalesce(t2.C_FIRST_SHIPTO_DATE_SK,0)
    ) 
THEN UPDATE SET
    end_date = current_date();

    
SELECT * FROM TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT;



create or replace table TPCDS.ANALYTICS.CUSTOMER_DIM as
        (select 
        C_SALUTATION,
        C_PREFERRED_CUST_FLAG,
        C_FIRST_SALES_DATE_SK,
        C_CUSTOMER_SK,
        C_LOGIN,
        C_CURRENT_CDEMO_SK,
        C_FIRST_NAME,
        C_CURRENT_HDEMO_SK,
        C_CURRENT_ADDR_SK,
        C_LAST_NAME,
        C_CUSTOMER_ID,
        C_LAST_REVIEW_DATE_SK,
        C_BIRTH_MONTH,
        C_BIRTH_COUNTRY,
        C_BIRTH_YEAR,
        C_BIRTH_DAY,
        C_EMAIL_ADDRESS,
        C_FIRST_SHIPTO_DATE_SK,
        CA_STREET_NAME,
        CA_SUITE_NUMBER,
        CA_STATE,
        CA_LOCATION_TYPE,
        CA_COUNTRY,
        CA_ADDRESS_ID,
        CA_COUNTY,
        CA_STREET_NUMBER,
        CA_ZIP,
        CA_CITY,
        CA_GMT_OFFSET,
        CD_DEP_EMPLOYED_COUNT,
        CD_DEP_COUNT,
        CD_CREDIT_RATING,
        CD_EDUCATION_STATUS,
        CD_PURCHASE_ESTIMATE,
        CD_MARITAL_STATUS,
        CD_DEP_COLLEGE_COUNT,
        CD_GENDER,
        HD_BUY_POTENTIAL,
        HD_DEP_COUNT,
        HD_VEHICLE_COUNT,
        HD_INCOME_BAND_SK,
        IB_LOWER_BOUND,
        IB_UPPER_BOUND,
        START_DATE,
        END_DATE
from TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT
LEFT JOIN TPCDS.RAW.customer_address ON c_current_addr_sk = ca_address_sk
LEFT join TPCDS.RAW.customer_demographics ON c_current_cdemo_sk = cd_demo_sk
LEFT join TPCDS.RAW.household_demographics ON c_current_hdemo_sk = hd_demo_sk
LEFT join TPCDS.RAW.income_band ON HD_INCOME_BAND_SK = IB_INCOME_BAND_SK
-- WHERE end_date IS NULL -- only add current records
        );  

-- show current records
SELECT * FROM TPCDS.ANALYTICS.CUSTOMER_DIM WHERE end_date IS NULL;


-- Showing cases where C_FIRST_SALES_DATE_SK is null

select count(*) from TPCDS.RAW.CUSTOMER where C_FIRST_SALES_DATE_SK is null; --3518 records

