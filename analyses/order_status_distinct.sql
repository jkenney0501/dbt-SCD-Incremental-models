SELECT DISTINCT(STATUS)
FROM {{ ref('stg_orders') }}