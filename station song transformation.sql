create database transformation;
use database transformation;
use schema public;



------------creaating stage-----------
create or replace stage raw_data
COPY_OPTIONS = (on_error='skip_file');


----------create file format to read csv file from raw_data-----------------
CREATE OR REPLACE FILE FORMAT CSV_FILE_FORMAT 
    TYPE =  CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '0x22';
    
    
------------------------viewing raw data------------
select 
  raw.$1 station_id, 
  raw.$2 song_id, 
  raw.$3 breakout_name, 
  raw.$4 date, 
  raw.$5 t1, 
  raw.$6 t2, 
  raw.$7 t3, 
  raw.$8 t4, 
  raw.$9 t5, 
  raw.$10 t6 
from 
  @raw_data/All_data.csv.gz 
  (file_format => CSV_FILE_FORMAT) raw;
  
  
---------------create raw table------------------
create or replace table raw_table as(
        select raw.$1 station_id,
            raw.$2 song_id,
            raw.$3 breakout_name,
            raw.$4 date,
            raw.$5 t1,
            raw.$6 t2,
            raw.$7 t3,
            raw.$8 t4,
            raw.$9 t5,
            raw.$10 t6
        from @raw_data/All_data.csv.gz (file_format => CSV_FILE_FORMAT) raw
    );
    
    
-- nepal = t1
--type_c = t6
-- neighbour = t2
--white = t3
--reality = t5
--bus = t4
--type_c(F) = 
-- type_c(M) = 
--asian = 
select  distinct breakout_name from raw_table;
select * from raw_table where breakout_name='Type_C (F)';



------------transformation sql----------
select station_id,
    song_id,
    sum(t1) t1,
    sum(t2) t2,
    sum(t3) t3,
    sum(t4) t4,
    sum(t5) t4
from raw_table
group by station_id,
    song_id;
    
-------------------------------cross verification--------------------------------
select * from raw_table where station_id = 3262323 and song_id = 1212122708;
select * from raw_table where station_id = 3381262 and song_id = 1215195611;
select * from raw_table where station_id = 3322798 and song_id = 84879771;
