USE saas_analytics; 


CREATE TABLE fact_revenue (
    revenue_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    subscription_id VARCHAR(50),
    revenue_date DATE,
    revenue_type VARCHAR(50),
    monthly_fee DECIMAL(10,2),
    amount DECIMAL(10,2),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (subscription_id) REFERENCES dim_subscriptions(subscription_id)
);

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/revenue.csv'
INTO TABLE fact_revenue
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    subscription_id,
    customer_id,
    @revenue_date,
    @monthly_fee,
    revenue_type,
    @amount
)
SET
    revenue_date = STR_TO_DATE(@revenue_date, '%Y-%m-%d'),
    monthly_fee = NULLIF(@monthly_fee, ''),
    amount = NULLIF(@amount, '');


SELECT COUNT(*) FROM fact_revenue; 

SELECT * 
FROM fact_revenue
LIMIT 5;

SELECT COUNT(*) 
FROM fact_revenue fr
LEFT JOIN dim_customers dc
  ON fr.customer_id = dc.customer_id
WHERE dc.customer_id IS NULL;


UPDATE fact_revenue
SET revenue_date = STR_TO_DATE(
    CONCAT(
        SUBSTRING(revenue_date, 1, 7),
        '-01'
    ),
    '%Y-%m-%d'
);

SELECT DISTINCT revenue_date
FROM fact_revenue
ORDER BY revenue_date
LIMIT 10;

