-- orders stage table

WITH 
    orders AS(
        SELECT 
            id as order_id,
            user_id as customer_id,
            order_date,
            -- add unit test and case when statement later
            status,
            CASE WHEN status LIKE '%pending%' THEN 'returned'
                 WHEN status LIKE '%returned%' THEN 'returned'
                 ELSE status
            END AS order_status
        FROM {{ source('jaffle_shop', 'orders') }}
)

SELECT *
FROM orders