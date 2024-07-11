-- stage for stripe payments

WITH 
    payments AS (
        SELECT * 
        FROM {{ source('stripe', 'payment') }}
)

SELECT * 
FROM payments