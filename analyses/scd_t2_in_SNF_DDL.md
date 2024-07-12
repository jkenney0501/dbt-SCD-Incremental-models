CREATE OR REPLACE TRANSIENT TABLE analytics.dbt_jkenney.mock_orders (
    order_id integer,
    status varchar (100),
    created_at date,
    updated_at date
);



INSERT INTO analytics.dbt_jkenney.mock_orders (order_id, status, created_at, updated_at)
VALUES (1, 'delivered', '2020-01-01', '2020-01-04'),
       (2, 'shipped', '2020-01-02', '2020-01-04'),
       (3, 'shipped', '2020-01-03', '2020-01-04'),
       (4, 'processed', '2020-01-04', '2020-01-04');
COMMIT;


--- mock orders sql
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




CREATE OR REPLACE TRANSIENT TABLE analytics.dbt_jkenney.mock_orders (
    order_id integer,
    status varchar (100),
    created_at date,
    updated_at date
);

INSERT INTO analytics.dbt_jkenney.mock_orders (order_id, status, created_at, updated_at)
VALUES (1, 'delivered', '2020-01-01', '2020-01-05'),
       (2, 'delivered', '2020-01-02', '2020-01-05'),
       (3, 'delivered', '2020-01-03', '2020-01-05'),
       (4, 'delivered', '2020-01-04', '2020-01-05');
COMMIT;


SELECT * FROM analytics.dbt_jkenney_snapshot.mock_orders