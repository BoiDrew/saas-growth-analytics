CREATE DATABASE saas_analytics;
USE saas_analytics;


CREATE TABLE dim_customers (
	customer_id INT PRIMARY KEY,
    signup_date DATE,
    plan_type VARCHAR(50),
    montlhy_fee DECIMAL(10,2),
    acquisiton_cost DECIMAL(10,2),
    churn_date DATE
);

CREATE TABLE dim_subscriptions (
	subscription_id INT PRIMARY KEY,
    customer_id INT,
    subscription_month DATE,
    monthly_fee DECIMAL(10,2),
    
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
);