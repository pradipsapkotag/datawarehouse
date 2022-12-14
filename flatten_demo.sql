create database json_flatten;

create schema json_flatten;

create or replace stage json_datas
COPY_OPTIONS = (on_error='skip_file');

----loading data to stage
-- put file:///home/prasag/Desktop/Fuse/datawarehouse/flatten_query_json_file/json_sample_data.json @json_datas;



list @json_datas;


------------creating raw table------------
create or replace table sample_json(
    raw_data VARIANT
    );


select * from JSON_FLATTEN.JSON_FLATTEN.SAMPLE_JSON;


-----------load data from stage to table---------
copy into JSON_FLATTEN.JSON_FLATTEN.SAMPLE_JSON
from @json_datas/json_sample_data.json.gz 
    file_format = (Type = JSON);



select * from JSON_FLATTEN.JSON_FLATTEN.SAMPLE_JSON;

------------query table and cast type----------------
select RAW_DATA:quiz:maths:q1:question::string Question
from JSON_FLATTEN.JSON_FLATTEN.SAMPLE_JSON;


----------query for sports question, options and answers------------
select VALUE:question::string sport_question,
    VALUE:options options,
    VALUE:answer::string answer
from JSON_FLATTEN.JSON_FLATTEN.SAMPLE_JSON,
    lateral flatten(INPUT => RAW_DATA:quiz:sport);
    


----------query for sports question, different options on different column and answers------------
select VALUE:question::string sport_question,
    VALUE:options[0]::string option_1,
    VALUE:options[1]::string option_2,
    VALUE:options[2]::string option_3,
    VALUE:options[3]::string option_4,
    VALUE:answer::string answer
from JSON_FLATTEN.JSON_FLATTEN.SAMPLE_JSON,
    lateral flatten(INPUT => RAW_DATA:quiz:sport);
    
    
    
---------select all from flatten------------
select *
from JSON_FLATTEN.JSON_FLATTEN.SAMPLE_JSON,
    lateral flatten(INPUT => RAW_DATA:quiz:sport);
    
    
    

---------select all from recursive flatten------------
select *
from JSON_FLATTEN.JSON_FLATTEN.SAMPLE_JSON,
    lateral flatten(INPUT => RAW_DATA:quiz, recursive => True);


-------------select question only------------
select value:question::string questions
from JSON_FLATTEN.JSON_FLATTEN.SAMPLE_JSON,
    lateral flatten(INPUT => RAW_DATA:quiz, recursive => True)
where questions is not null;
    
    
    
----------select question options and answer------------
select value:question::string questions,
    value:options [0]::string option_1,
    value:options [1]::string option_2,
    value:options [2]::string option_3,
    value:options [3]::string option_4,
    value:answer::string answer
from JSON_FLATTEN.JSON_FLATTEN.SAMPLE_JSON,
    lateral flatten(INPUT => RAW_DATA:quiz, recursive => True)
where questions is not null;
    
    
-----------inserting into table question_answer--------------------
create or replace table JSON_FLATTEN.JSON_FLATTEN.question_answer as(
        select value:question::string questions,
            value:options[0]::string option_1,
            value:options[1]::string option_2,
            value:options[2]::string option_3,
            value:options[3]::string option_4,
            value:answer::string answer
        from JSON_FLATTEN.JSON_FLATTEN.SAMPLE_JSON,
            lateral flatten(INPUT => RAW_DATA:quiz, recursive => True)
        where questions is not null
    );
    
    
select * from JSON_FLATTEN.JSON_FLATTEN.QUESTION_ANSWER;
    
