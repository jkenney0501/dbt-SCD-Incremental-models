-- Note: a table has to exist in the DWH previoud to attemping to measure the increments.

-- Creates an incremental model for a fact table

-- create a new source yml and updaet it to reflect the freshness with error after and warn after  - source_inc.yml
-- configure the model at the table level here, materiallize as table
-- make sure to identify the primary key and timestamp to measure if there are new records 

{{ config(
    materialized = 'incremental', 
    unique_key = 'payment_id',
    on_schema_change = 'fail'
) }}


-- create the table and add incrmental logic
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