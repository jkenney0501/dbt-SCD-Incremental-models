
-- set up for dbt cloud, this is a quick set up to start using the dbt cloud env

USE ACCOUNTADMIN; -- typically we use another role bu this is for dev purpoess - SCD/incrmental models and this simplifies creting a user etc...

create or replace database raw;

create or replace database analytics;

create or replace schema raw.jaffle_shop;

create or replace schema raw.stripe;



USE ROLE TRANSFORM;

-- creates tables ddl
create or replace table raw.jaffle_shop.customers 
( id integer,
  first_name varchar,
  last_name varchar
);

-- insert data into customers from s3
copy into raw.jaffle_shop.customers (id, first_name, last_name)
from 's3://dbt-tutorial-public/jaffle_shop_customers.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    ); 

-- check load, all good
SELECT * 
FROM RAW.JAFFLE_SHOP.CUSTOMERS LIMIT 20;



-- create table orders
create or replace table raw.jaffle_shop.orders
( id integer,
  user_id integer,
  order_date date,
  status varchar,
  _etl_loaded_at timestamp default current_timestamp
);

-- insert data into orders from s3
copy into raw.jaffle_shop.orders (id, user_id, order_date, status)
from 's3://dbt-tutorial-public/jaffle_shop_orders.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    );

-- check load 
SELECT *
FROM raw.jaffle_shop.orders limit 20;


-- create tbale for payments
create or replace table raw.stripe.payment 
( id integer,
  orderid integer,
  paymentmethod varchar,
  status varchar,
  amount integer,
  --created_at timestamp,
  _batched_at timestamp default current_timestamp
);

-- insert data for payments from s3
copy into raw.stripe.payment (id, orderid, paymentmethod, status, amount, _batched_at)
from 's3://dbt-tutorial-public/stripe_payments.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    );
    
-- CHECK LOAD
SELECT *
FROM RAW.STRIPE.PAYMENT limit 20;















