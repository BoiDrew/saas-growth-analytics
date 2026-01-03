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

ALTER TABLE dim_customers
RENAME COLUMN montlhy_fee TO monthly_fee;
ALTER TABLE dim_customers
RENAME COLUMN acquisiton_cost TO acquisition_cost;

CREATE TABLE dim_subscriptions (
    subscription_id VARCHAR(50) PRIMARY KEY,
    customer_id INT,
    subscription_month DATE,
    monthly_fee DECIMAL(10,2),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
);


/* 
   LOAD DATA SECTION 
*/
# Load dim_customers
LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/customers.csv'
INTO TABLE dim_customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    customer_id,
    @signup_date,
    plan_type,
    @montlhy_fee,
    @acquisition_cost,
    @churn_date
)
SET
    signup_date = STR_TO_DATE(@signup_date, '%Y-%m-%d'),
    monthly_fee = NULLIF(@monthly_fee, ''),
    acquisition_cost = NULLIF(@acquisition_cost, ''),
    churn_date = NULLIF(@churn_date, '');
    

# Load the dim_subscriptions

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/subscriptions.csv'
INTO TABLE dim_subscriptions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    subscription_id,
    customer_id,
    @subscription_month,
    @monthly_fee
)
SET
    subscription_month = STR_TO_DATE(@subscription_month, '%Y-%m-%d'),
    monthly_fee = NULLIF(@monthly_fee, '');


SELECT COUNT(*) FROM dim_customers;
SELECT COUNT(*) FROM dim_subscriptions;
SELECT COUNT(*) FROM fact_revenue;

SELECT COUNT(*) 
FROM dim_customers 
WHERE signup_date IS NULL;


SELECT COUNT(*) 
FROM fact_revenue 
WHERE amount IS NULL AND revenue_type = 'subscription';



#Total Customers
SELECT COUNT(*) AS total_customers
FROM dim_customers;

# Activiated Customers
SELECT COUNT(DISTINCT customer_id) AS activated_customers
FROM fact_revenue;


#Activation Rate (SQL)
SELECT
    ROUND(
        COUNT(DISTINCT fr.customer_id) * 1.0 /
        (SELECT COUNT(*) FROM dim_customers),
        4
    ) AS activation_rate
FROM fact_revenue fr;


# Early Churn First 90 Days
SELECT
    ROUND(
        SUM(
            CASE
                WHEN churn_date IS NOT NULL
                 AND churn_date <= DATE_ADD(signup_date, INTERVAL 90 DAY)
                THEN 1 ELSE 0
            END
        ) * 1.0 / COUNT(*),
        4
    ) AS early_churn_rate
FROM dim_customers;


#AVG DAYS FIRST REVENUE
SELECT
    ROUND(AVG(months_to_first_revenue), 2) AS avg_months_to_first_revenue
FROM (
    SELECT
        dc.customer_id,
        TIMESTAMPDIFF(
            MONTH,
            DATE_FORMAT(dc.signup_date, '%Y-%m-01'),
            MIN(fr.revenue_date)
        ) AS months_to_first_revenue
    FROM dim_customers dc
    JOIN fact_revenue fr
      ON dc.customer_id = fr.customer_id
    GROUP BY dc.customer_id, dc.signup_date
) t;

