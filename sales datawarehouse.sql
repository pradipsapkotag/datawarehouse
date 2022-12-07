-------------------------------------dataset link-----------------------------------
-- https://www.kaggle.com/datasets/rohitsahoo/sales-forecasting?datasetId=835308

create database sales;


create schema raw_sales_data;


create schema sales_warehouse;


use schema SALES.RAW_SALES_DATA;


------------creaating stage-----------
create or replace stage raw_sales_data
COPY_OPTIONS = (on_error='skip_file');




list @raw_sales_data;


-----------------------------------------load data to stage-----------------------------------

-- put file:///home/prasag/Desktop/Fuse/datawarehouse/dataset/salesdata.csv @raw_sales_data;



----------create file format to read csv file from raw_data_stage-----------------


CREATE OR REPLACE FILE FORMAT CSV_FILE_FORMAT 
    TYPE =  CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '0x22';
    
----------------------------------------------------------------------------columns from raw data--------------------------------------------------------------------------

--Row ID,Order ID,Order Date,Ship Date,Ship Mode,Customer ID,Customer Name,Segment,Country,City,State,Postal Code,Region,Product ID,Category,Sub-Category,Product Name,Sales


------------------------viewing raw data------------
select 
  sales.$1 "Row ID", 
  sales.$2 "Order ID", 
  sales.$3 "Order Date", 
  sales.$4 "Ship Date", 
  sales.$5 "Ship Mode", 
  sales.$6 "Customer ID", 
  sales.$7 "Customer Name", 
  sales.$8 "Segment", 
  sales.$9 "Country", 
  sales.$10 "City", 
  sales.$11 "State", 
  sales.$12 "Postal Code",
  sales.$13 "Region",
  sales.$14 "Product ID",
  sales.$15 "Category",
  sales.$16 "Sub-Category",
  sales.$17 "Product Name",
  sales.$18 "Sales"
from 
  @raw_sales_data/salesdata.csv.gz 
  (file_format => CSV_FILE_FORMAT) sales limit 100;
  
  
  
  
  
---------------------creating table to store raw data-------------------
create or replace table SALES.RAW_SALES_DATA.raw_sales_data(
        "Row ID" Integer,
        "Order ID" string,
        "Order Date" date,
        "Ship Date" date,
        "Ship Mode" string,
        "Customer ID" string,
        "Customer Name" string,
        "Segment" string,
        "Country" string,
        "City" string,
        "State" string,
        "Postal Code" integer,
        "Region" string,
        "Product ID" string,
        "Category" string,
        "Sub-Category" string,
        "Product Name" string,
        "Sales" float
    );


select * from SALES.RAW_SALES_DATA.RAW_SALES_DATA;


-----------insert data into table from stage-----------

copy into SALES.RAW_SALES_DATA.RAW_SALES_DATA
from @raw_sales_data/salesdata.csv.gz 
    file_format = (format_name = CSV_FILE_FORMAT) 
    on_error = "continue";









--------------creating orders dimension table------------------

create or replace table SALES.SALES_WAREHOUSE.orders_dim(
    "PK" integer primary key,
    "Order ID" string,
    "Order Date" date,
    "Ship Date" date,
    "Ship Mode" string

);



---------------------insert into criteria statement for orders dimension------------------------------
insert into SALES.SALES_WAREHOUSE.ORDERS_DIM with distinct_orders as (
        select distinct "Order ID",
            "Order Date",
            "Ship Date",
            "Ship Mode"
        from SALES.RAW_SALES_DATA.RAW_SALES_DATA
    )
select row_number() over (
        order by "Order ID"
    ) as "PK",
    "Order ID",
    "Order Date",
    "Ship Date",
    "Ship Mode"
from distinct_orders;




-------------insert into orders dimension using stored procedure-----------

create or replace procedure stp_orders_dim()
  returns string 
  language javascript
  as
  $$  
  var My_query = `insert into SALES.SALES_WAREHOUSE.ORDERS_DIM with distinct_orders as (
                    select distinct "Order ID",
                        "Order Date",
                        "Ship Date",
                        "Ship Mode"
                    from SALES.RAW_SALES_DATA.RAW_SALES_DATA
                )
            select row_number() over (
                    order by "Order ID"
                ) as "PK",
                "Order ID",
                "Order Date",
                "Ship Date",
                "Ship Mode"
            from distinct_orders`;

  var sql_statement = snowflake.createStatement({
  	sqlText: My_query
  });

  var result = sql_statement.execute();
  var  st = 'success'
  return st;
  $$
  ;



select * from SALES.SALES_WAREHOUSE.ORDERS_DIM;


call stp_orders_dim();


select * from SALES.SALES_WAREHOUSE.ORDERS_DIM;










--------------creating customer dimension table------------------

create or replace table SALES.SALES_WAREHOUSE.customer_dim(
    "PK" integer primary key,
    "Customer ID" string,
    "Customer Name" string
);



---------------------insert into criteria statement for customer dimension------------------------------
insert into SALES.SALES_WAREHOUSE.CUSTOMER_DIM with distinct_customers as (
        select distinct "Customer ID",
            "Customer Name"
        from SALES.RAW_SALES_DATA.RAW_SALES_DATA
    )
select row_number() over (
        order by "Customer ID"
    ) as "PK",
    "Customer ID",
    "Customer Name"
from distinct_customers;



-------------insert into customer dimension using stored procedure-----------

create or replace procedure stp_customer_dim()
  returns string 
  language javascript
  as
  $$  
  var My_query = `insert into SALES.SALES_WAREHOUSE.CUSTOMER_DIM with distinct_customers as (
                    select distinct "Customer ID",
                        "Customer Name"
                    from SALES.RAW_SALES_DATA.RAW_SALES_DATA
                    )
                select row_number() over (
                        order by "Customer ID"
                    ) as "PK",
                    "Customer ID",
                    "Customer Name"
                from distinct_customers`;

  var sql_statement = snowflake.createStatement({
  	sqlText: My_query
  });

  var result = sql_statement.execute();
  var  st = 'success'
  return st;
  $$
  ;




select * from SALES.SALES_WAREHOUSE.CUSTOMER_DIM;

call stp_customer_dim();

select * from SALES.SALES_WAREHOUSE.CUSTOMER_DIM;














--------------creating address dimension table------------------

create or replace table SALES.SALES_WAREHOUSE.address_dim(
    "Postal Code" integer primary key,
    "Country" string,
    "City" string,
    "State" string,
    "Region" string
);





---------------------insert into criteria statement for address dimension------------------------------
insert into SALES.SALES_WAREHOUSE.ADDRESS_DIM
select distinct "Postal Code",
    "Country",
    "City",
    "State",
    "Region"
from SALES.RAW_SALES_DATA.RAW_SALES_DATA
WHERE "Postal Code" IS NOT NULL;
    

-------------insert into address dimension using stored procedure-----------

create or replace procedure stp_address_dim()
  returns string 
  language javascript
  as
  $$  
  var My_query = `insert into SALES.SALES_WAREHOUSE.ADDRESS_DIM
                    select distinct "Postal Code",
                        "Country",
                        "City",
                        "State",
                        "Region"
                    from SALES.RAW_SALES_DATA.RAW_SALES_DATA
                    WHERE "Postal Code" IS NOT NULL`;

  var sql_statement = snowflake.createStatement({
  	sqlText: My_query
  });

  var result = sql_statement.execute();
  var  st = 'success'
  return st;
  $$
  ;


select * from SALES.SALES_WAREHOUSE.ADDRESS_DIM;

call stp_address_dim();

select * from SALES.SALES_WAREHOUSE.ADDRESS_DIM;










--------------creating product dimension table------------------
--product id and product name commnly can be used as primary key
create or replace table SALES.SALES_WAREHOUSE.product_dim(
    "PK" integer primary key,
    "Product ID" string,
    "Product Name" string,
    "Category" string,
    "Sub-Category" string
);



---------------------insert into criteria statement for product dimension------------------------------
insert into SALES.SALES_WAREHOUSE.PRODUCT_DIM with distinct_product as (
        select distinct "Product ID",
            "Product Name",
            "Category",
            "Sub-Category"
        from SALES.RAW_SALES_DATA.RAW_SALES_DATA
    )
select row_number() over (
        order by "Product ID"
    ) as "PK",
    "Product ID",
    "Product Name",
    "Category",
    "Sub-Category"
from distinct_product;



-------------insert into product dimension using stored procedure-----------

create or replace procedure stp_product_dim()
  returns string 
  language javascript
  as
  $$  
  var My_query = `insert into SALES.SALES_WAREHOUSE.PRODUCT_DIM with distinct_product as (
                        select distinct "Product ID",
                            "Product Name",
                            "Category",
                            "Sub-Category"
                        from SALES.RAW_SALES_DATA.RAW_SALES_DATA
                        )
                select row_number() over (
                        order by "Product ID"
                    ) as "PK",
                    "Product ID",
                    "Product Name",
                    "Category",
                    "Sub-Category"
                from distinct_product`;

  var sql_statement = snowflake.createStatement({
  	sqlText: My_query
  });

  var result = sql_statement.execute();
  var  st = 'success'
  return st;
  $$
  ;


select * from SALES.SALES_WAREHOUSE.PRODUCT_DIM;

call stp_product_dim();

select * from SALES.SALES_WAREHOUSE.PRODUCT_DIM;











--------------creating segment dimension table------------------

create or replace table SALES.SALES_WAREHOUSE.segment_dim(
    "PK" integer primary key,
    "Segment" string
);


---------------------insert into criteria statement for segment dimension------------------------------
insert into SALES.SALES_WAREHOUSE.SEGMENT_DIM with distinct_segment as (
        select distinct "Segment" from SALES.RAW_SALES_DATA.RAW_SALES_DATA
    )
select row_number() over (
        order by "Segment"
    ) as "PK",
    "Segment"
from distinct_segment;



select * from SALES.SALES_WAREHOUSE.SEGMENT_DIM;



















-----------------------------for Fact table-------------------------------------------------



--------------creating address dimension table------------------

create or replace table SALES.SALES_WAREHOUSE.SALES_FACT(
    "Row ID" integer primary key,
    "Order FK" integer references SALES.SALES_WAREHOUSE.ORDERS_DIM("PK"),
    "Customer FK" integer references SALES.SALES_WAREHOUSE.CUSTOMER_DIM("PK"),
    "Address FK" integer references SALES.SALES_WAREHOUSE.ADDRESS_DIM("Postal Code"),
    "Product FK" integer references SALES.SALES_WAREHOUSE.PRODUCT_DIM("PK"),
    "Segment FK" integer references SALES.SALES_WAREHOUSE.SEGMENT_DIM("PK"),
    "Sales" Float
);

--------------insertion criteria for sales fact--------------------------
insert into SALES.SALES_WAREHOUSE.SALES_FACT(
        select raw."Row ID",
            orders."PK" as "Order FK",
            customer."PK" as "Customer FK",
            address."Postal Code" as "Address FK",
            product."PK" as "Product FK",
            segment."PK" as "Segment FK",
            raw."Sales"
        from RAW_SALES_DATA as raw
            join SALES.SALES_WAREHOUSE.ADDRESS_DIM as address on raw."Postal Code" = address."Postal Code"
            join SALES.SALES_WAREHOUSE.CUSTOMER_DIM as customer on raw."Customer ID" = customer."Customer ID"
            join SALES.SALES_WAREHOUSE.ORDERS_DIM as orders on raw."Order ID" = orders."Order ID"
            join SALES.SALES_WAREHOUSE.PRODUCT_DIM as product on raw."Product ID" = product."Product ID"
            and raw."Product Name" = product."Product Name"
            join SALES.SALES_WAREHOUSE.SEGMENT_DIM as segment on raw."Segment" = segment."Segment"
    );
  
  
  -------------insert into sales fact using stored procedure-----------

create or replace procedure stp_sales_fact()
  returns string 
  language javascript
  as
  $$  
  var My_query = `insert into SALES.SALES_WAREHOUSE.SALES_FACT(
                        select raw."Row ID",
                            orders."PK" as "Order FK",
                            customer."PK" as "Customer FK",
                            address."Postal Code" as "Address FK",
                            product."PK" as "Product FK",
                            segment."PK" as "Segment FK",
                            raw."Sales"
                        from RAW_SALES_DATA as raw
                            join SALES.SALES_WAREHOUSE.ADDRESS_DIM as address on raw."Postal Code" = address."Postal Code"
                            join SALES.SALES_WAREHOUSE.CUSTOMER_DIM as customer on raw."Customer ID" = customer."Customer ID"
                            join SALES.SALES_WAREHOUSE.ORDERS_DIM as orders on raw."Order ID" = orders."Order ID"
                            join SALES.SALES_WAREHOUSE.PRODUCT_DIM as product on raw."Product ID" = product."Product ID"
                            and raw."Product Name" = product."Product Name"
                            join SALES.SALES_WAREHOUSE.SEGMENT_DIM as segment on raw."Segment" = segment."Segment"
                    )`;

  var sql_statement = snowflake.createStatement({
  	sqlText: My_query
  });

  var result = sql_statement.execute();
  var  st = 'success'
  return st;
  $$
  ;
    
    
    
select * from SALES.SALES_WAREHOUSE.SALES_FACT;

call stp_sales_fact();

select * from SALES.SALES_WAREHOUSE.SALES_FACT;




select * from RAW_SALES_DATA limit 10;
select * from SALES.SALES_WAREHOUSE.ADDRESS_DIM limit 10;
select * from SALES.SALES_WAREHOUSE.CUSTOMER_DIM limit 10;
select * from SALES.SALES_WAREHOUSE.ORDERS_DIM limit 10;
select * from SALES.SALES_WAREHOUSE.PRODUCT_DIM limit 10;
select * from SALES.SALES_WAREHOUSE.SEGMENT_DIM limit 10;
select * from SALES.SALES_WAREHOUSE.SALES_FACT limit 10;
