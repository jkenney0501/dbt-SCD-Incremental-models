-- customers stage table

WITH customers AS(
SELECT 
    id as customer_id,
    first_name,
    last_name
FROM {{ source('jaffle_shop', 'customers') }}
)

SELECT *
FROM customers