use database HISTORICAL_SALES;
show schemas;
use schema HISTORICAL_SALES;
show tables;




-------------get highest temperature---------------------
create or replace procedure highest_temperature(table_name VARCHAR)
  returns string 
  language javascript
  as
  $$  
  var My_query = `select "Temperature" from ` + TABLE_NAME + ` order  by "Temperature" desc limit 1 `;

  var sql_statement = snowflake.createStatement({
  	sqlText: My_query
  });

  var result = sql_statement.execute();
  result.next();
  var valuee = result.getColumnValue(1)
  return valuee;
  $$
  ;
  
  
  
CALL highest_temperature('Features');



  
  

  
  


-----------------create total sales table-----------------
create 
or replace table total_sales(
    "Store" integer, 
    total_sales float
);

select * from total_sales;


----------selection criteria statement------------
select 
    sales."Store", 
    sum(sales."Weekly_Sales") as total_sales 
  from 
    HISTORICAL_SALES.HISTORICAL_SALES."Sales" sales 
  group by 
    sales."Store";



-----------insert into table total_sales from table Sales with store id and total accumulated sales---------------- 
create or replace procedure total_sales()
  returns string 
  language javascript
  as
  $$  
     var My_query = `
                        select 
                            sales."Store", 
                            sum(sales."Weekly_Sales") as total_sales 
                          from 
                            HISTORICAL_SALES.HISTORICAL_SALES."Sales" sales 
                          group by 
                            sales."Store"`;

    var sql_statement = snowflake.createStatement({
        sqlText: My_query
    });
    var result = sql_statement.execute();
    while (result.next()) {
        var storeid = result.getColumnValue(1);
        var total_sales = result.getColumnValue(2);
        var stmt = snowflake.createStatement({
            sqlText: "INSERT INTO total_sales VALUES (?, ?);",
            binds: [storeid, total_sales]
        });
        stmt.execute();
    }
    return "success";
  $$;


select * from total_sales;

------calling procedure----------
call total_sales();



select * from total_sales;







