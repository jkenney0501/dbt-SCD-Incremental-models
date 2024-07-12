-- this is essentially a mock dimension, typically we pull from the source.
-- choose strategy - check or timestamp. If you have a reliable timestamp, use that, its more accurate.
-- invalidate hard deletes makes sure that deletes are captured with an end date.

{% snapshot scd_mock_orders %}

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