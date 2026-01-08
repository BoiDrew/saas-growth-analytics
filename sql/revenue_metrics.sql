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

# monthly MRR
SELECT
    revenue_date,
    ROUND(SUM(amount), 2) AS mrr
FROM fact_revenue
GROUP BY revenue_date
ORDER BY revenue_date;


#Revenue Retention by Cohort 
WITH cohort_revenue AS (
    SELECT
        DATE_FORMAT(dc.signup_date, '%Y-%m-01') AS cohort_month,
        TIMESTAMPDIFF(
            MONTH,
            DATE_FORMAT(dc.signup_date, '%Y-%m-01'),
            fr.revenue_date
        ) AS months_since_signup,
        SUM(fr.amount) AS cohort_revenue
    FROM dim_customers dc
    JOIN fact_revenue fr
      ON dc.customer_id = fr.customer_id
    GROUP BY cohort_month, months_since_signup
),
cohort_base AS (
    SELECT
        cohort_month,
        cohort_revenue AS month0_revenue
    FROM cohort_revenue
    WHERE months_since_signup = 0
)
SELECT
    cr.cohort_month,
    cr.months_since_signup,
    cr.cohort_revenue,
    ROUND(
        cr.cohort_revenue * 1.0 / cb.month0_revenue,
        4
    ) AS revenue_retention_rate
FROM cohort_revenue cr
JOIN cohort_base cb
  ON cr.cohort_month = cb.cohort_month
ORDER BY cr.cohort_month, cr.months_since_signup;


# ARPU (Average Revenue Per User)
SELECT
    fr.revenue_date,
    ROUND(
        SUM(fr.amount) / COUNT(DISTINCT fr.customer_id),
        2
    ) AS arpu
FROM fact_revenue fr
GROUP BY fr.revenue_date
ORDER BY fr.revenue_date;

# Avg revenue per active customer
SELECT
    ROUND(AVG(amount), 2) AS avg_monthly_revenue_per_customer
FROM fact_revenue;


# Avg active lifespan (months)
SELECT
    ROUND(AVG(active_months), 2) AS avg_active_months
FROM (
    SELECT
        customer_id,
        COUNT(DISTINCT revenue_date) AS active_months
    FROM fact_revenue
    GROUP BY customer_id
) t;

