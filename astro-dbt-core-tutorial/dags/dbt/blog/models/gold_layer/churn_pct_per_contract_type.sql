SELECT
    contract,
    internet_service,
    phone_service,    
    internet_type,
    SUM(Total_Revenue)/SUM(tenure_in_months) as monthly_revenue,
    ((sum(case when customer_status = 'Churned' then 1 else 0 end)/count(*))*100) as churn_percentage
FROM POC_AIRFLOW_DBT.BRONZE_LAYER.telecom_customer_churn
GROUP BY
    contract,
    internet_service,
    internet_type,
    phone_service
ORDER BY 
    contract,
    internet_service,
    internet_type,
    phone_service