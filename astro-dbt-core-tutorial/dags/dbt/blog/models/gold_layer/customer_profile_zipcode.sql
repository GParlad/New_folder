with population_zip_code as (
    SELECT *
    FROM {{ ref('stg_zip_code') }} 
),

customer_profile as (
    SELECT * FROM POC_AIRFLOW_DBT.BRONZE_LAYER.telecom_customer_churn
),

final as (
SELECT 
    zip_code,
    COUNT(customer_id) as total_customers,
    AVG(Age) as average_age, 
    AVG(number_of_dependents) as average_no_dependents_per_customer,
    ((sum(case when Gender = 'Male' then 1 else 0 end)/count(*))*100) as male_percentage,
    ((sum(case when Gender = 'Female' then 1 else 0 end)/count(*))*100) as female_percentage,
    ((sum(case when phone_service = 'True' then 1 else 0 end)/count(*))*100) as phone_service_percentage,
    ((sum(case when internet_service = 'True' then 1 else 0 end)/count(*))*100) as internet_service_percentage,
    ((sum(case when internet_service = 'True' or phone_service = 'True' then 1 else 0 end)/max(population))*100) as service_percentage_over_population
FROM POC_AIRFLOW_DBT.BRONZE_LAYER.telecom_customer_churn
LEFT JOIN population_zip_code USING (zip_code)
GROUP BY zip_code
)
SELECT * FROM final