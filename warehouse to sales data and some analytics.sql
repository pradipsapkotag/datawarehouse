use warehouse practice_warehouse;
use database sales;
use schema sales_warehouse;

----------------select from sales fact table-------
select * from SALES.SALES_WAREHOUSE.SALES_FACT;

------------------select from raw sales data----------
select * from SALES.RAW_SALES_DATA.RAW_SALES_DATA;


-----------------raw data from sales fact and other dimensions------------
select fact."Row ID",
    orders."Order ID" as "Order ID",
    orders."Order Date" as "Order Date",
    orders."Ship Date" as "Ship Date",
    orders."Ship Mode" as "Ship Mode",
    customer."Customer ID" as "Customer ID",
    customer."Customer Name" as "Customer Name",
    segment."Segment" as "Segment",
    address."Country" as "Country",
    address."City" as "City",
    address."State" as "State",
    address."Postal Code" as "Postal Code",
    address."Region" as "Region",
    product."Product ID" as "Product ID",
    product."Category" as "Category",
    product."Sub-Category" as "Sub-Category",
    product."Product Name" as "Product Name",
    fact."Sales" as "Sales"
from SALES.SALES_WAREHOUSE.SALES_FACT as fact
    join SALES.SALES_WAREHOUSE.ADDRESS_DIM as address on fact."Address FK" = address."Postal Code"
    join SALES.SALES_WAREHOUSE.CUSTOMER_DIM as customer on fact."Customer FK" = customer."PK"
    join SALES.SALES_WAREHOUSE.ORDERS_DIM as orders on fact."Order FK" = orders."PK"
    join SALES.SALES_WAREHOUSE.PRODUCT_DIM as product on fact."Product FK" = product."PK"
    join SALES.SALES_WAREHOUSE.SEGMENT_DIM as segment on fact."Segment FK" = segment."PK";
    

-----------------creating raw view-------------------------
create or replace view raw as (
        select fact."Row ID",
            orders."Order ID" as "Order ID",
            orders."Order Date" as "Order Date",
            orders."Ship Date" as "Ship Date",
            orders."Ship Mode" as "Ship Mode",
            customer."Customer ID" as "Customer ID",
            customer."Customer Name" as "Customer Name",
            segment."Segment" as "Segment",
            address."Country" as "Country",
            address."City" as "City",
            address."State" as "State",
            address."Postal Code" as "Postal Code",
            address."Region" as "Region",
            product."Product ID" as "Product ID",
            product."Category" as "Category",
            product."Sub-Category" as "Sub-Category",
            product."Product Name" as "Product Name",
            fact."Sales" as "Sales"
        from SALES.SALES_WAREHOUSE.SALES_FACT as fact
            join SALES.SALES_WAREHOUSE.ADDRESS_DIM as address on fact."Address FK" = address."Postal Code"
            join SALES.SALES_WAREHOUSE.CUSTOMER_DIM as customer on fact."Customer FK" = customer."PK"
            join SALES.SALES_WAREHOUSE.ORDERS_DIM as orders on fact."Order FK" = orders."PK"
            join SALES.SALES_WAREHOUSE.PRODUCT_DIM as product on fact."Product FK" = product."PK"
            join SALES.SALES_WAREHOUSE.SEGMENT_DIM as segment on fact."Segment FK" = segment."PK"
    );
    

------------viewing from view-------------
select * from SALES.SALES_WAREHOUSE.RAW;

select * from SALES.RAW_SALES_DATA.RAW_SALES_DATA;







--1---------------------------sales by date---------------------
with data as (
    select fact.*,
        orders."Order Date"
    from SALES.SALES_WAREHOUSE.SALES_FACT as fact
        join SALES.SALES_WAREHOUSE.ORDERS_DIM as orders on fact."Order FK" = orders."PK"
)
select "Order Date",
    sum("Sales") as "Accumulated Sales"
from data
group by "Order Date"
order by "Order Date";





--2----------------total sales by year--------------------------
with data as (
    select fact.*,
        orders."Order Date"
    from SALES.SALES_WAREHOUSE.SALES_FACT as fact
        join SALES.SALES_WAREHOUSE.ORDERS_DIM as orders on fact."Order FK" = orders."PK"
)
select year("Order Date") as "Year",
    sum("Sales") as "Total Sales"
from data
group by year("Order Date")
order by "Year";





--3-----------------------product name of top ten selled products-----------------------
select products."Product ID",
    products."Product Name",
    sum(fact."Sales") as "Total Sales"
from SALES.SALES_WAREHOUSE.SALES_FACT as fact
    join SALES.SALES_WAREHOUSE.PRODUCT_DIM as products on products.pk = fact."Product FK"
group by products."Product ID",
    products."Product Name"
order by "Total Sales" desc
limit 10;





--4----products(name,ID) which are shipped to Chicago City in the year 2018--------------
select year(orders."Order Date") as "Year",
    product."Product ID",
    product."Product Name",
    address."City"
from SALES.SALES_WAREHOUSE.SALES_FACT as fact
    join SALES.SALES_WAREHOUSE.ORDERS_DIM as orders on orders.pk = fact."Order FK"
    join SALES.SALES_WAREHOUSE.PRODUCT_DIM as product on product.pk = fact."Product FK"
    join SALES.SALES_WAREHOUSE.ADDRESS_DIM as address on address."Postal Code" = fact."Address FK"
where address."City" = 'Chicago'
    and "Year" = 2018; 
    
    
    
    
    
    
--5----------------20 most famous product in winter(december,january,fabuary) season------------

select product."Product ID",
    product."Product Name",
    Sum(fact."Sales") as "Total Sales"
from SALES.SALES_WAREHOUSE.SALES_FACT as fact
    join SALES.SALES_WAREHOUSE.ORDERS_DIM as orders on orders.pk = fact."Order FK"
    join SALES.SALES_WAREHOUSE.PRODUCT_DIM as product on product.pk = fact."Product FK"
where month(orders."Order Date") in (12, 1, 2)
group by product."Product ID",
    product."Product Name"
order by "Total Sales" desc
limit 20;
