use warehouse practice_warehouse;
create database table_swap;
use database table_swap;
use schema public;


create or replace table table1 (
        name string,
        email string,
        number string
    );

select * from TABLE_SWAP.PUBLIC.TABLE1;

insert into TABLE_SWAP.PUBLIC.TABLE1 (name, email, number)
values('AP', 'ap@gmail.com', '9840449184'),
    ('AP', 'apaaa@gmail.com', '9840373649'),
    ('AP', 'apppp@gmail.com', '9840128367'),
    ('BC', 'bc@gmail.com', '9840337266'),
    ('BC', 'cb@gmail.com', '9840337266'),
    ('BC', 'bcb@gmail.com', '9840333323'),
    ('BD', 'bd@gmail.com', '9840098779'),
    ('BD', 'db@gmail.com', '9840336387'),
    ('BD', 'bdbd@gmail.com', '9840388273'),
    ('BA', 'ba@gmail.com', '9840456789'),
    ('BA', 'bababa@gmail.com', '9840456789'),
    ('BA', 'bababa@gmail.com', '9840377689');

select * from TABLE_SWAP.PUBLIC.TABLE1;

create or replace table table2 (
        name string,
        email string,
        number string
    );
    

select * from TABLE_SWAP.PUBLIC.TABLE2;



INSERT INTO table2 (name, email, number)
VALUES (
        'AP',
        'ap@gmail.com,apaaa@gmail.com,apppp@gmail.com',
        '9840449184,9840373649,9840128367'
    ),
    (
        'BC',
        'bc@gmail.com,cb@gmail.com,bcb@gmail.com',
        '9840337266,9840337266,9840333323'
    ),
    (
        'BD',
        'bd@gmail.com,db@gmail.com,bdbd@gmail.com',
        '9840098779,9840336387,9840388273'
    ),
    (
        'BA',
        'ba@gmail.com,bababa@gmail.com',
        '9840456789,9840377689'
    );
    
select * from TABLE_SWAP.PUBLIC.TABLE2;



-------------------table1 transformation--------------------
select name,
    listagg(email, ', ') email,
    listagg(number, ', ') number
from TABLE_SWAP.PUBLIC.TABLE1
group by name;





-------------------table2 transformation--------------------

select name,
    email.value::string email
from TABLE_SWAP.PUBLIC.TABLE2,
    lateral flatten(input => split(email, ',')) email;

     
     
     
 
-------- method 1----------------
select name,
    email.value email,
    number.value number
from TABLE_SWAP.PUBLIC.TABLE2,
    lateral flatten(input => split(number, ',')) number,
    lateral flatten(input => split(email, ',')) email
where email.index = number.index;
     



-------------method 2----------------------------

SELECT name,
    email.value email,
    number.value number
FROM TABLE_SWAP.PUBLIC.TABLE2,
    LATERAL SPLIT_TO_TABLE(email, ',') AS email,
    LATERAL SPLIT_TO_TABLE(number, ',') AS number
WHERE email.index = number.index;
