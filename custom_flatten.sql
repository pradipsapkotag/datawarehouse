list @json_datas;



-- put file:///home/prasag/Desktop/Fuse/datawarehouse/dataset/udemy_courses.json @json_datas;

------------creating raw table------------
create or replace table JSON_FLATTEN.JSON_FLATTEN.udemy_courses_raw(
    udemy_raw VARIANT
    );
    
    

-----------load data from stage to table---------
copy into JSON_FLATTEN.JSON_FLATTEN.UDEMY_COURSES_RAW
from @json_datas/udemy_courses.json.gz 
    file_format = (Type = JSON);



---------------select all loded rows-----------------
-- added rows = 3668
select * from JSON_FLATTEN.JSON_FLATTEN.UDEMY_COURSES_RAW;





----------query for sports question, options and answers------------
select
    UDEMY_RAW:course_id::string course_id,
    UDEMY_RAW:course_title::string course_title,
    UDEMY_RAW:url::string url,
    UDEMY_RAW:is_paid::string is_paid,
    UDEMY_RAW:price::string price,
    UDEMY_RAW:num_subscribers::string num_subscribers,
    UDEMY_RAW:num_reviews::string num_reviews,
    UDEMY_RAW:num_lectures::string num_lectures,
    UDEMY_RAW:level::string level,
    UDEMY_RAW:content_duration::string content_duration,
    UDEMY_RAW:published_timestamp::string published_timestamp,
    UDEMY_RAW:subject::string subject
from JSON_FLATTEN.JSON_FLATTEN.UDEMY_COURSES_RAW;




-------------------------insert into udemy_courses table---------------
create or replace table JSON_FLATTEN.JSON_FLATTEN.udemy_courses as(
        select UDEMY_RAW:course_id::string course_id,
            UDEMY_RAW:course_title::string course_title,
            UDEMY_RAW:url::string url,
            UDEMY_RAW:is_paid::string is_paid,
            UDEMY_RAW:price::string price,
            UDEMY_RAW:num_subscribers::string num_subscribers,
            UDEMY_RAW:num_reviews::string num_reviews,
            UDEMY_RAW:num_lectures::string num_lectures,
            UDEMY_RAW:level::string level,
            UDEMY_RAW:content_duration::string content_duration,
            UDEMY_RAW:published_timestamp::string published_timestamp,
            UDEMY_RAW:subject::string subject
        from JSON_FLATTEN.JSON_FLATTEN.UDEMY_COURSES_RAW
);


select * from JSON_FLATTEN.JSON_FLATTEN.UDEMY_COURSES;











----------------------for users json file----------------------------------

--put file:///home/prasag/Desktop/Fuse/datawarehouse/dataset/users.json @json_datas;

------------creating raw table------------
create or replace table JSON_FLATTEN.JSON_FLATTEN.users_raw(
    users_raw VARIANT
    );


-----------load data from stage to table---------
copy into JSON_FLATTEN.JSON_FLATTEN.USERS_RAW
from @json_datas/users.json.gz 
    file_format = (Type = JSON);
    
    

---------------select all loded rows-----------------
-- added rows = 10
select * from JSON_FLATTEN.JSON_FLATTEN.USERS_RAW;


---------select all all useful data from recursive flatten------------
select users_raw:id user_id,
    users_raw:name::string name,
    users_raw:username::string username,
    users_raw:email::string email,
    value:street::string street,
    value:suite::string suite,
    value:city::string city,
    value:geo:lat::float latitude,
    value:geo:lng::float longitude,
    users_raw:phone::string phone
from JSON_FLATTEN.JSON_FLATTEN.USERS_RAW,
    lateral flatten(INPUT => USERS_RAW, recursive => True)
where city is not null;



-------------insert useful data into table-------------------------
create or replace table JSON_FLATTEN.JSON_FLATTEN.users as(
        select users_raw:id user_id,
            users_raw:name::string name,
            users_raw:username::string username,
            users_raw:email::string email,
            value:street::string street,
            value:suite::string suite,
            value:city::string city,
            value:geo:lat::float latitude,
            value:geo:lng::float longitude,
            users_raw:phone::string phone
        from JSON_FLATTEN.JSON_FLATTEN.USERS_RAW,
            lateral flatten(INPUT => USERS_RAW, recursive => True)
        where city is not null
    );
    
    
    
select * from JSON_FLATTEN.JSON_FLATTEN.USERS;


remove @json_datas/udemy_courses.json.gz;