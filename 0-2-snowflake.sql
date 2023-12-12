--Validate Cloud Region and User Info
select current_region(), current_account(), current_user(), current_role(), current_warehouse(), current_database();

/* GUI - Create initial database 

use role sysadmin; 
create or replace database citibike; 
use database citibike;
use warehouse compute_wh;
*/


-- GUI - Create worksheet and set proper context. Name worksheet Zero to Snowflake
-- Loading Data, Creating tables, getting started 
create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);

/*
GUI - Click into Databases and look at the new database and table that we just loaded data into.
 */

 /* GUI create external stage. Make sure to include the final forward slash (/) at the end of the URL or you will encounter errors later when loading data from the bucket

create stage citibike_trips
    url = s3://snowflake-workshop-lab/citibike-trips-csv/;
*/

-- peak into the external stage we just made. Note this is looking at storage external to snowflake.
list @citibike_trips;


 -- Create file formate for import and view it. 
create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';

show file formats in database citibike;

/* GUI under admin menus select compute_wh and up the size from x-small to small (doubleling compute)

alter warehouse compute_wh set warehouse_size='small';
*/

-- Copy data into table using Small WH and view 10 results - Note loading time 
-- Role: SYSADMIN Warehouse: COMPUTE_WH Database: CITIBIKE Schema = PUBLIC
copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;


select * from trips limit 10;


-- Clear out the table and verify it is empty 
truncate table trips;
select * from trips limit 10;

-- Change size of wh and look at all warehouses, notice how fast the change is.
alter warehouse compute_wh set warehouse_size='large';
show warehouses;

-- Lets reload the CSV data with the larger warehouse, how much faster did it run with the larger warehouse 
copy into trips from @citibike_trips
file_format=CSV;

-- GUI navigate back to queries page and examine the 2 copy commands.

-- We Now have data ready to rock. Lets configure an analytics enviornment and take actions as that persona. 
/* GUI - Lets create the warehouse for our analyst via point and click.

create or replace warehouse analytics_wh    
    warehouse_size = large
    auto_suspend = 300
    auto_resume = true 
    min_cluster_count = 1
    max_cluster_count = 4
    initially_suspended = false;  
*/


-- Time to run some queries
-- First look as an analyist 
use role sysadmin;
use warehouse analytics_wh;
select * from trips limit 20;

-- Look at trips by hour including average trip distance and duration. Run this twice to notice the improvments from the results cache. 
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

-- 2nd time for results cache. 
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

-- Let us look at the busiest months. 
select
monthname(starttime) as "month",
count(*) as "num trips"
from trips
group by 1 order by 2 desc;

-- Time to creat a Zero Copy Clone to create a table for development pourposes. 
-- Notice no differences between the tables
create table trips_dev clone trips; 


-- To pull in weather data that we know is semi-structured we need to create a database. 
create database weather;

-- Set context for the upcomming work. 
use role sysadmin;
use warehouse compute_wh;
use database weather;
use schema public;

-- create a table with a variant column that can take our json files. 
create table json_weather_data (v variant);


-- Lets create another external stage, this time using SQL and inspect it
create stage nyc_weather
url = 's3://snowflake-workshop-lab/zero-weather-nyc';

-- inspect stage
list @nyc_weather;

select $1,$2,$3 from @nyc_weather limit 10;


-- load and  the semi-structured data
copy into json_weather_data
from @nyc_weather 
    file_format = (type = json strip_outer_array = true);

-- Verify loaded data    
select * from json_weather_data limit 10;


-- create a view that will put structure onto the semi-structured data
create or replace view json_weather_data_view as
select
    v:obsTime::timestamp as observation_time,
    v:station::string as station_id,
    v:name::string as city_name,
    v:country::string as country,
    v:latitude::float as city_lat,
    v:longitude::float as city_lon,
    v:weatherCondition::string as weather_conditions,
    v:coco::int as weather_conditions_code,
    v:temp::float as temp,
    v:prcp::float as rain,
    v:tsun::float as tsun,
    v:wdir::float as wind_dir,
    v:wspd::float as wind_speed,
    v:dwpt::float as dew_point,
    v:rhum::float as relative_humidity,
    v:pres::float as pressure
from
    json_weather_data
where
    station_id = '72502'; --station for newark airport 

-- look at the structure with this sample query 
select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;


-- using a join on the structured trips table and the semi-structured weather view. 
-- showing us weather influences bike trips 
select weather_conditions as conditions
,count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;


-- time to make some mistakes 
drop table json_weather_data;

-- Prove its gone 
select * from json_weather_data limit 10;

-- bring it back 
undrop table json_weather_data;

-- Prove its back 
select * from json_weather_data limit 10;

-- set context, preparing to make a bigger mistake. 
use role sysadmin;
use warehouse compute_wh;
use database citibike;
use schema public;

-- Bigger mistake, all stations are now called oops
update trips set start_station_name = 'oops';

-- validating that indeed all stations are called oops 
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;


-- Rather than point in time recovery, we can issue a query to find our mistake 
set query_id =
(select query_id from table(information_schema.query_history_by_session (result_limit=>5))
where query_text like 'update%' order by start_time desc limit 1);

-- Look at the variable we made just for fun 
select $query_id;

-- use that fun variable to rollback our mistake
create or replace table trips as
(select * from trips before (statement => $query_id));

-- re-run the query that proved our oops in the first place 
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;


 -- Playing with roles, lets become and admin and create a junior role to give to ourselves. 
use role accountadmin;
create role junior_dba;

set my_user=
(select current_user);

select $my_user;

grant role junior_dba to user <COPYPASTE USER HERE>;
use role junior_dba;
-- notice our new role cant see a warehouse ? we should change that 


-- Switch back to admin 
use role accountadmin;
 -- Grant access to the warehouse
grant usage on warehouse compute_wh to role junior_dba;

 -- Prove we can now use the warehouse 
use role junior_dba;
use warehouse compute_wh;


-- Now that our junior dba can use a warehouse, lets make sure they can use some data as well
use role accountadmin;
grant usage on database citibike to role junior_dba;
grant usage on database weather to role junior_dba;


-- Become junior dba and see that we now have permissions to compute and data 
use role junior_dba;

-- Let us look at some of the usage of the lab thus far
use role accountadmin;




-- Time to do some interactive data sharing, lets become accountadmin. 
use role accountadmin;


create database citishare clone citibike;

use warehouse compute_wh;
use database citishare;
use schema public;

set my_user=
(select current_user);

update trips_dev set start_station_name = $my_user;

select start_station_name from trips_dev limit 10;




-- GUI for point and click data sharing
-- Ensure you know your account identifier


--  replace with the name you used for the share
show shares;

drop share if exists <MY_COOL_SHARE>;

-- Drop everything else
drop database if exists citibike;
drop database if exists weather;
drop warehouse if exists analytics_wh;
drop role if exists junior_dba;






