## Incremental Models and Type 2 SCD's in dbt
This first set of code captures how to create and maintain a tpye 2 SCD in dbt. 
It exapands further to simulate how we would create a production dimesnion by transforning and adding the is_current flag.

The other example is to materialize an incremental model. This is essentially a mock fact table and a few rows are added to 
demonstrate the incrmenting appraoch. Typiclly, this is best used for large table and most often a rebuild is best if you can get away with it.

## dbt snapshots - Type 2 Slowly Changing Dimensions 
- why we use 
- tow strtegies in dbt 
- briefly cover check cols strategy 

**Steps to create an SCD:**
- **(In Snowflake)** Create a table called mock_orders in your development schema. You will have to replace dbt_kcoapman in the snippet below.

```sql
CREATE OR REPLACE TRANSIENT TABLE analytics.dbt_jkenney.mock_orders (
    order_id integer,
    status varchar (100),
    created_at date,
    updated_at date
);
```
- **(In Snowflake)** Insert values into the mock_orders table in your development schema. You will have to replace dbt_kcoapman in the snippet below.

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
      invalidate_hard_delets=True
    )
}}

select * from analytics.{{target.schema}}.mock_orders

{% endsnapshot %}

```
- **Note:** The above code creates a separate schema for the type 2 SCD's once a change occurs.
- **(In dbt Cloud)** Run snapshots by executing **dbt snapshot**.

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

screenshot this

create dim with transformed showing valid to/from and is current flag

**Note:** If you want to start this process over, you will need to drop the snapshot table by running the following in Snowflake. 
This will force dbt to create a new snapshot table in step 4. Or you can keep adding records to capture new changes.

```sql
DROP TABLE analytics.dbt_jkenney_snapshot.mock_orders
```

## Incremental Models



## Start with config

```sql
{{ config(
    materialized = 'incremental',
    unique_key = 'page_view_id'
) }}
```

### Using the starter project

Try running the following commands:
- dbt run
- dbt test














### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
