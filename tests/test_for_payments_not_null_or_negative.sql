-- singular test to check tha payments are at least 0 and not null, this can be done in schema.yml as well

WITH 
    payments_test AS (
        SELECT * 
        FROM {{ source('stripe', 'payment') }}
        WHERE amount IS NULL OR amount < 0
)

SELECT * 
FROM payments_test