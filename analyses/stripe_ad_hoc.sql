SELECT 
MAX(id) AS max_id,
MAX(orderid) AS max_orderid
FROM {{ source('stripe', 'payment') }}