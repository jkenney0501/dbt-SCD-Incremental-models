
-- transforms the dim with alias and captrures the is current falg by using the null in dbt valid to
WITH dim_transformed_scd AS(
SELECT 
order_id,
status,
dbt_updated_at AS updated_at,
dbt_valid_from AS valid_from,
dbt_valid_to AS valid_to,
CASE WHEN dbt_valid_to IS NULL THEN 1 ELSE 0 END AS is_current
FROM {{ ref('scd_mock_orders') }}
),
--  fills the null of dbt valid to with a date far in the future, this happens AFTER the is current flag is defined so it does not over write it.
dim_transform_valid_to AS(
    SELECT 
        order_id,
        status,
        valid_from,
        CASE WHEN valid_to IS NULL THEN '2099-01-01' ELSE valid_to END AS valid_to,
        updated_at,
        is_current
    FROM dim_transformed_scd
)

SELECT *
FROM dim_transform_valid_to