SELECT
    city as City,
    COUNT(Customer_id) as Number_of_customers,
    SUM(Total_Charges) as Total_Contractual_Charges,
    SUM(Total_Extra_Data_Charges) + SUM(Total_Long_Distance_Charges) as Total_Extra_Charges,
    SUM(Total_Refunds) as Total_Refunds,
    SUM(Total_Revenue) as Total_Revenue
FROM POC_AIRFLOW_DBT.BRONZE_LAYER.telecom_customer_churn
GROUP BY city