create warehouse practice_warehouse;

create database historical_sales;

create schema historical_sales;

use database HISTORICAL_SALES;
use schema HISTORICAL_SALES;


----------creaating stages---------
create or replace stage first_stage 
COPY_OPTIONS = (on_error='skip_file');


create or replace stage second_stage 
COPY_OPTIONS = (on_error='skip_file');




SHOW STAGES;

-- put file:///home/prasag/Downloads/Dataset_1/Dataset_1/features.csv @FIRST_STAGE;

-- put file:///home/prasag/Downloads/Dataset_1/Dataset_1/sales.csv @FIRST_STAGE;

-- put file:///home/prasag/Downloads/Dataset_1/Dataset_1/stores.csv @SECOND_STAGE;



LIST @FIRST_STAGE;

LIST @SECOND_STAGE;

--create file format named CSV_FILE_FORMAT
CREATE 
OR REPLACE FILE FORMAT CSV_FILE_FORMAT TYPE =  CSV  SKIP_HEADER = 1;





-----------------For featres table--------------------



                      
--Store	Date	Temperature	Fuel_Price	MarkDown1	MarkDown2	MarkDown3	MarkDown4	MarkDown5	CPI	Unemployment	IsHoliday
select 
  features.$1 "Store", 
  features.$2 "Date", 
  features.$3 "Temperature", 
  features.$4 "Fuel_Price", 
  features.$5 "MarkDown1", 
  features.$5 "MarkDown2", 
  features.$7 "MarkDown3", 
  features.$8 "MarkDown4", 
  features.$9 "MarkDown5", 
  features.$10 "CPI", 
  features.$11 "Unemployment", 
  features.$12 "IsHoliday" 
from 
  @FIRST_STAGE/features.csv.gz 
  (file_format => CSV_FILE_FORMAT) features ;
  
drop table HISTORICAL_SALES.HISTORICAL_SALES."Features";





------------------- create features table--------------------------

CREATE OR REPLACE TABLE HISTORICAL_SALES.HISTORICAL_SALES."Features"(
"Store" Integer,
    "Date"	String,
    "Temperature" Float,
    "Fuel_Price" Float,
    "MarkDown1" String,
    "MarkDown2"	String,
    "MarkDown3" String,
    "MarkDown4"	String,
    "MarkDown5"	String,
    "CPI" String,
    "Unemployment" String,
    "IsHoliday" String

);


  
select * from HISTORICAL_SALES.HISTORICAL_SALES."Features";


-- to load data into features table from first satge features.csv file
copy into HISTORICAL_SALES.HISTORICAL_SALES."Features" 
from 
  @FIRST_STAGE/features.csv.gz 
  file_format = (format_name = CSV_FILE_FORMAT) 
  on_error = "continue";


select * from HISTORICAL_SALES.HISTORICAL_SALES."Features";
  

drop table HISTORICAL_SALES.HISTORICAL_SALES."Features";



------create features table from select statement-------------
create or replace  table HISTORICAL_SALES.HISTORICAL_SALES."Features" AS 
(
    select 
  features.$1 "Store", 
  features.$2 "Date", 
  features.$3 "Temperature", 
  features.$4 "Fuel_Price", 
  features.$5 "MarkDown1", 
  features.$5 "MarkDown2", 
  features.$7 "MarkDown3", 
  features.$8 "MarkDown4", 
  features.$9 "MarkDown5", 
  features.$10 "CPI", 
  features.$11 "Unemployment", 
  features.$12 "IsHoliday" 
from 
  @FIRST_STAGE/features.csv.gz 
  (file_format => CSV_FILE_FORMAT) features 
    
    
);

select * from HISTORICAL_SALES.HISTORICAL_SALES."Features";
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  ----------------------------------for sales table-------------------------
  
  
  
  CREATE OR REPLACE TABLE HISTORICAL_SALES.HISTORICAL_SALES."Sales"(
    "Store" Integer,
    "Dept" Integer,
    "Date"	String,
    "Weekly_Sales" Float,
    "IsHoliday" String

);
  
  select * from HISTORICAL_SALES.HISTORICAL_SALES."Sales";
  
  
  
  
  
--Store	Dept	Date	Weekly_Sales	IsHoliday
select 
  sales.$1 "Store", 
  sales.$2 "Dept", 
  sales.$3 "Date", 
  sales.$4 "Weekly_Sales", 
  sales.$5 "IsHoliday" 
from 
  @FIRST_STAGE/sales.csv.gz (file_format => CSV_FILE_FORMAT) sales;
  
  
select * from HISTORICAL_SALES.HISTORICAL_SALES."Sales";
  
  
  --load data into sales table from first stafe file sales.csv
copy into HISTORICAL_SALES.HISTORICAL_SALES."Sales" 
from 
  @FIRST_STAGE/sales.csv.gz 
  file_format = (format_name = CSV_FILE_FORMAT) 
  on_error = "continue";
  
  

select * from HISTORICAL_SALES.HISTORICAL_SALES."Sales";











--------------for stores table------------------

CREATE OR REPLACE TABLE HISTORICAL_SALES.HISTORICAL_SALES."Stores"(
    "Store" Integer,
    "Type"	String,
    "Size" Integer

);

select * from HISTORICAL_SALES.HISTORICAL_SALES."Stores";



--Store	Type	Size

select 
  stores.$1 "Store", 
  stores.$2 "Type", 
  stores.$3 "Size"
from 
  @SECOND_STAGE/stores.csv.gz (file_format => CSV_FILE_FORMAT) stores;
  
  
  
  
  select * from HISTORICAL_SALES.HISTORICAL_SALES."Stores";
  
  
-----load data into sales table from second stafe file stores.csv
copy into HISTORICAL_SALES.HISTORICAL_SALES."Stores" 
from 
  @SECOND_STAGE/stores.csv.gz 
  file_format = (format_name = CSV_FILE_FORMAT) 
  on_error = "continue";
  
  
  
select * from HISTORICAL_SALES.HISTORICAL_SALES."Stores";
  
  
drop table HISTORICAL_SALES.HISTORICAL_SALES."Stores";
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
----------------------------for views---------------------






-----------------------aggregration statement---------------------
select 
  sales."Store", 
  sum(sales."Weekly_Sales") as total_sales 
from 
  HISTORICAL_SALES.HISTORICAL_SALES."Sales" sales 
group by 
  sales."Store";

  
  
  
  
  ---------------create total sales for stores-------------------------- 
create 
or replace view HISTORICAL_SALES.HISTORICAL_SALES."total_sales" as (
  select 
    sales."Store", 
    sum(sales."Weekly_Sales") as total_sales 
  from 
    HISTORICAL_SALES.HISTORICAL_SALES."Sales" sales 
  group by 
    sales."Store"
);

  
  
select * from HISTORICAL_SALES.HISTORICAL_SALES."total_sales";








select 
  sls."Store", 
  stor."Size", 
  sls."Date", 
  sls."Weekly_Sales" 
from 
  HISTORICAL_SALES.HISTORICAL_SALES."Sales" sls 
  inner join HISTORICAL_SALES.HISTORICAL_SALES."Stores" stor on sls."Store" = stor."Store";



-----------------------------view included join----------------------------------------------
create 
or replace view HISTORICAL_SALES.HISTORICAL_SALES."depart_sales_with_size" as (
  select 
    sls."Store", 
    stor."Size", 
    sls."Date", 
    sls."Weekly_Sales" 
  from 
    HISTORICAL_SALES.HISTORICAL_SALES."Sales" sls 
    inner join HISTORICAL_SALES.HISTORICAL_SALES."Stores" stor on sls."Store" = stor."Store"
);



select * from HISTORICAL_SALES.HISTORICAL_SALES."depart_sales_with_size";














----------------------------Tasks--------------------------------------
create or replace stage task_stage 
COPY_OPTIONS = (on_error='skip_file');

list @task_stage;

--put file:///home/prasag/Desktop/Fuse/DataWarehouse/Dataset/student.csv @task_stage;

---view what is in student.scv file in task_stage------
select 
  student.$1 "ID", 
  student.$2 "name", 
  student.$3 "dept_name", 
  student.$4 "tot_cred"
from 
  @TASK_STAGE/student.csv.gz (file_format => CSV_FILE_FORMAT) student;
  
  
-------------------------create student table--------------------------
create 
or replace table HISTORICAL_SALES.HISTORICAL_SALES."Students"(
    "ID" String,
    "name" string,
    "dept_name" string, 
    "tot_cred" string
);
  


-----------------create task to insert into student table -----------------------------------
create or replace task add_student 
    warehouse = PRACTICE_WAREHOUSE schedule = 'USING CRON */1 * * * * America/Los_Angeles'
    AS INSERT INTO HISTORICAL_SALES.HISTORICAL_SALES."Students"
    SELECT 
      student.$1 "ID", 
      student.$2 "name", 
      student.$3 "dept_name", 
      student.$4 "tot_cred" 
    FROM 
      @TASK_STAGE/student.csv.gz (file_format => CSV_FILE_FORMAT) student;



-------------load data into students table from task_stage --------------------
copy into HISTORICAL_SALES.HISTORICAL_SALES."Students" 
from 
  @TASK_STAGE/student.csv.gz 
  file_format = (format_name = CSV_FILE_FORMAT) 
  on_error = "continue";
  
  
select * from HISTORICAL_SALES.HISTORICAL_SALES."Students";



-----------resume or suspend task------------
ALTER TASK add_student RESUME;

-------for suspend--------
ALTER TASK add_student SUSPEND;




select * from HISTORICAL_SALES.HISTORICAL_SALES."Students";
select count(*) as "total counts" from HISTORICAL_SALES.HISTORICAL_SALES."Students";




-------------------------to view task history----------------------------------

SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'ADD_STUDENT'));







