--
USE ROLE SYSADMIN;

-- Create our database for raw data
CREATE DATABASE IF NOT EXISTS dbt_dbtFundamentals_RAW;

-- Create our database for analytics
CREATE DATABASE IF NOT EXISTS dbt_dbtFundamentals_DW;

-- Setup db for loading data into raw db tables
USE WAREHOUSE COMPUTE_WH;
USE DATABASE dbt_dbtFundamentals_RAW;



--- 
create schema dbt_dbtFundamentals_RAW.jaffle_shop;
create schema dbt_dbtFundamentals_RAW.stripe;

-- customer
create table dbt_dbtFundamentals_RAW.jaffle_shop.customers 
( id integer,
  first_name varchar,
  last_name varchar
);
copy into dbt_dbtFundamentals_RAW.jaffle_shop.customers (id, first_name, last_name)
from 's3://dbt-tutorial-public/jaffle_shop_customers.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    ); 
select * from dbt_dbtFundamentals_RAW.jaffle_shop.customers;
    
-- orders
create table dbt_dbtFundamentals_RAW.jaffle_shop.orders
( id integer,
  user_id integer,
  order_date date,
  status varchar,
  _etl_loaded_at timestamp default current_timestamp
);
copy into dbt_dbtFundamentals_RAW.jaffle_shop.orders (id, user_id, order_date, status)
from 's3://dbt-tutorial-public/jaffle_shop_orders.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    );
select * from dbt_dbtFundamentals_RAW.jaffle_shop.orders;

-- payment
create table dbt_dbtFundamentals_RAW.stripe.payment 
( id integer,
  orderid integer,
  paymentmethod varchar,
  status varchar,
  amount integer,
  created date,
  _batched_at timestamp default current_timestamp
);
copy into dbt_dbtFundamentals_RAW.stripe.payment (id, orderid, paymentmethod, status, amount, created)
from 's3://dbt-tutorial-public/stripe_payments.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    );
select * from dbt_dbtFundamentals_RAW.stripe.payment;
