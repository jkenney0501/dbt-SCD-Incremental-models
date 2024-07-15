## Incremental Models and Type 2 SCD's in dbt
This first set of code captures how to create and maintain a tpye 2 SCD in dbt. 
It exapands further to simulate how we would create a production dimesnion by transforning and adding the is_current flag.

The other example is to materialize an incremental model. This is essentially a mock fact table and a few rows are added to 
demonstrate the incrmenting appraoch. Typiclly, this is best used for large table and most often a rebuild is best if you can get away with it.

Get the [jaffle_shop DDL](https://github.com/jkenney0501/dbt-SCD-Incremental-models/blob/main/analyses/jaffle_set_up_SNF_DDL.md) in the Analyses folder.

## dbt snapshots - Type 2 Slowly Changing Dimensions 
We use type 2 SCD's to capture the history of changes. A type 2 will track the entore history while remaining idempotent which is very important for 
building good data pieplines. Below covers the two strategies that dbt uses, timestamp and check. 
Timestamp is the preferred methid if we have a reloable timestamp column but we can alos employ the check strategy.
Both methods are covwered below as is incremental modeling. 

**Steps to create an SCD:**
- **(In Snowflake)** Create a table called mock_orders in your development schema. You will have to replace dbt_jkenney in the snippet below.

```sql
CREATE OR REPLACE TRANSIENT TABLE analytics.dbt_jkenney.mock_orders (
    order_id integer,
    status varchar (100),
    created_at date,
    updated_at date
);
```
- **(In Snowflake)** Insert values into the mock_orders table in your development schema. 

```sql
INSERT INTO analytics.dbt_jkenney.mock_orders (order_id, status, created_at, updated_at)
VALUES (1, 'delivered', '2020-01-01', '2020-01-04'),
       (2, 'shipped', '2020-01-02', '2020-01-04'),
       (3, 'shipped', '2020-01-03', '2020-01-04'),
       (4, 'processed', '2020-01-04', '2020-01-04');
COMMIT;
```
- **(In dbt Cloud)** Create a new snapshot in the folder snapshots with the filename mock_orders.sql with the following code snippet. Note: Jinja is being used here to create a new, dedicated schema.

```sql
{% snapshot mock_orders %}

{% set new_schema = target.schema + '_snapshot' %}

{{
    config(
      target_database='analytics',
      target_schema=new_schema,
      unique_key='order_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

select * from analytics.{{target.schema}}.mock_orders

{% endsnapshot %}

```

- **(In dbt Cloud)** Run snapshots by executing **dbt snapshot**.
- **Note:** The above code creates a separate schema for the type 2 SCD's once a change occurs.

- **(In dbt Cloud)** Run the following snippet in a statement tab to see the current snapshot table.

```sql 
SELECT * 
FROM  analytics.dbt_jkenney_snapshot.mock_orders 
```

- **(In Snowflake)** *Recreate* a table called mock_orders in your development schema. (Drop the old table if that wasnt obvious).
```sql
CREATE OR REPLACE TRANSIENT TABLE analytics.dbt_jkenney.mock_orders (
    order_id integer,
    status varchar (100),
    created_at date,
    updated_at date
);
```
- **(In Snowflake)** Insert these new values into the mock_orders table in your development schema.

```sql
INSERT INTO analytics.dbt_jkenney.mock_orders (order_id, status, created_at, updated_at)
VALUES (1, 'delivered', '2020-01-01', '2020-01-05'),
       (2, 'delivered', '2020-01-02', '2020-01-05'),
       (3, 'delivered', '2020-01-03', '2020-01-05'),
       (4, 'delivered', '2020-01-04', '2020-01-05');
COMMIT;
```

- **(In dbt Cloud)** Re-run snapshots by executing **dbt snapshot**.
- **(In dbt Cloud)** Run the following snippet in a statement tab to see the current snapshot table. 

```sql
SELECT * FROM analytics.dbt_jkenney_snapshot.mock_orders
```

### Create Final Dimension with Timestamp Strategy (this will referece the scd snapshot)

- Now we create the final dimension where we transform the raw(ish) SCD schema to have an is_current flag and fill in NULLS with a future date(way in the future)
 
Here is how I did this section:
```sql
-- transforms the dim with alias and captrures the is current falg by using the null in dbt valid to
WITH 
    dim_transformed_scd AS(
        SELECT 
            order_id,
            status,
            dbt_updated_at AS updated_at,
            dbt_valid_from AS valid_from,
            dbt_valid_to AS valid_to,
            CASE WHEN dbt_valid_to IS NULL THEN 1 ELSE 0 END AS is_current
        FROM {{ ref('scd_mock_orders') }}
),
--  fills the null of dbt valid to with a date far in the future
-- this happens AFTER the is current flag is defined so it does not over write it.
dim_transform_valid_to AS(
    SELECT 
        order_id,
        status,
        valid_from,
        CASE WHEN valid_to IS NULL THEN '2099-01-01' ELSE valid_to END AS valid_to,
        updated_at,
        is_current
    FROM dim_transformed_scd
)

SELECT *
FROM dim_transform_valid_to
```
- enter **dbt run** to build model in DWH. Optional-(dbt run--select <model_name>) 
- check this transformed dimesion:

```sql
SELECT * FROM ANALYTICS.DBT_JKENNEY.DIM_MOCK_ORDERS_SCD
```
![Alt Text](https://github.com/jkenney0501/dbt-SCD-Incremental-models/blob/main/assets/timestamp_scd.png)

**Note:** If you want to start this process over, you will need to drop the snapshot table by running the following in Snowflake. 
This will force dbt to create a new snapshot table in step 4. Or you can keep adding records to capture new changes.

```sql
DROP TABLE analytics.dbt_jkenney_snapshot.mock_orders
```

### Using the Check Columns strategy
Given our customers source does not have any time stamp that we can use, we will check for chnages in any of the columns to update that row as current
if there is a change. To do this we have top modify our config a bit as I did below:

**Note:** I am using the source here b/c it is part of my loaded data and I would typically always do this but I chose to follow the dbt guide for the above timestamp strategy which did not use a source.

```sql
{% snapshot scd_check_customers %}

{{
    config(
      target_schema='dbt_jkenney_snapshot',
      strategy='check',
      unique_key='id',
      check_cols='all'
    )
}}

SELECT * FROM {{ source('jaffle_shop', 'customers') }}

{% endsnapshot %}
```
-- enter **dbt snapshot**

- Check the table.
```sql 
SELECT * FROM ANALYTICS.DBT_JKENNEY_SNAPSHOT.SCD_CHECK_CUSTOMERS
```
### Create Final Dimension with Check Strategy (this will referece the scd snapshot)
- Now create the dim table as above with the is current flag as seen below.
```sql 
WITH 
    dim_cust_transformed_scd AS(
        SELECT 
            id AS customer_id,
            first_name,
            last_name,
            dbt_updated_at AS updated_at,
            dbt_valid_from AS valid_from,
            dbt_valid_to AS valid_to,
            CASE WHEN dbt_valid_to IS NULL THEN 1 ELSE 0 END AS is_current
        FROM {{ ref('scd_check_customers') }}
),
--  fills the null of dbt valid to with a date far in the future, this happens AFTER the is current flag is defined so it does not over write it.
dim_cust_transform_valid_to AS(
    SELECT 
        customer_id,
        first_name,
        last_name,
        valid_from,
        CASE WHEN valid_to IS NULL THEN '2099-01-01' ELSE valid_to END AS valid_to,
        updated_at,
        is_current
    FROM dim_cust_transformed_scd
)

SELECT *
FROM dim_cust_transform_valid_to
```
- enter **dbt run** to build model in DWH.
- make a change in snowflake to the source data for a customers name where id =  1 change first_name to Mike

```sql
UPDATE RAW.JAFFLE_SHOP.CUSTOMERS
SET first_name = 'Mike'
WHERE id = 1
```
- Check the results

```sql
SELECT * 
FROM ANALYTICS.DBT_JKENNEY.DIM_CUSTOMERS_SCD
WHERE customer_id =  1
```
- You will now see a cleaned up version of an SCD with a **is_currrent** flag and the valid_to date as 2099-01-01 
which make it easier to query using a range.
![Alt Text](https://github.com/jkenney0501/dbt-SCD-Incremental-models/blob/main/assets/CHECK_STRATEGY_SCD.png)



### Incremental Models


- Start with configuring the materialization as Incremental
- In the same sql file, add the CTE below and thats really it for incremental models!

```sql
{{ config(
    materialized = 'incremental',
    unique_key = 'page_view_id',
    on_schema_change ='fail'
) }}

-- create the table and add incrmental logic
-- this cte references the source data which is perefectly fine to do.
WITH 
    incremental AS(
        SELECT 
            id AS payment_id, 
            orderid AS order_id,
            amount,
            _batched_at
        FROM {{ ref('stg_payment') }}
     
        {% if is_incremental() %}

            WHERE _batched_at >= (SELECT MAX(_batched_at) FROM {{ this }} )

        {% endif %}
)

SELECT * 
FROM incremental
```

- Insert some new data into the raw source table in Snowflake
```sql
INSERT INTO RAW.STRIPE.PAYMENT (id, orderid, paymentmethod, status, amount, _batched_at)
VALUES (121, 100, 'credit_card', 'success', 5000, current_timestamp)

-- CHECK RAW
SELECT *
FROM RAW.STRIPE.PAYMENT
ORDER BY id DESC

-- run dbt run --full-refresh

-- check fact table in SNF, the run is quick b/s it adds only 1 row! Repeat above as needed.
-- you will see order id 121 is added.
SELECT *
FROM ANALYTICS.DBT_JKENNEY.FCT_PAYMENTS_INC
ORDER BY payment_id DESC
```
- Note, this is essentially the fact table, it cleans up some cols from the source that it references (elimiates them) and aliases some columns.
- You can also add surrogate keys but that was not neccesary for purposes of this demonstration.




### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
