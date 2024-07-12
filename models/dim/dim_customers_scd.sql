-- transforms the dim with alias and captrures the is current falg by using the null in dbt valid to
WITH 
    dim_cust_transformed_scd AS(
        SELECT 
            id AS customer_id,
            first_name,
            last_name,
            dbt_updated_at AS updated_at,
            dbt_valid_from AS valid_from,
            dbt_valid_to AS valid_to,
            CASE WHEN dbt_valid_to IS NULL THEN 1 ELSE 0 END AS is_current
        FROM {{ ref('scd_check_customers') }}
),
--  fills the null of dbt valid to with a date far in the future, this happens AFTER the is current flag is defined so it does not over write it.
dim_cust_transform_valid_to AS(
    SELECT 
        customer_id,
        first_name,
        last_name,
        valid_from,
        CASE WHEN valid_to IS NULL THEN '2099-01-01' ELSE valid_to END AS valid_to,
        updated_at,
        is_current
    FROM dim_cust_transformed_scd
)

SELECT *
FROM dim_cust_transform_valid_to