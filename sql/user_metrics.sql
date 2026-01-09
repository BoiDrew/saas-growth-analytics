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


# Cohort = month of sign up

SELECT
	DATE_FORMAT(signup_date, '%Y-%m-01') AS cohort_month,
    COUNT(*) AS customer_signed_up
FROM dim_customers
GROUP BY cohort_month
ORDER BY cohort_month;


SELECT
    DATE_FORMAT(dc.signup_date, '%Y-%m-01') AS cohort_month,
    COUNT(DISTINCT fr.customer_id) AS activated_customers
FROM dim_customers dc
JOIN fact_revenue fr
  ON dc.customer_id = fr.customer_id
GROUP BY cohort_month
ORDER BY cohort_month;

SELECT 
	DATE_FORMAT(dc.signup_date, '%Y-%m-01') AS cohort_month,
    TIMESTAMPDIFF(
		MONTH,
        DATE_FORMAT(dc.signup_date, '%Y-%m-01'),
        fr.revenue_date
	) AS months_since_signup,
    COUNT(DISTINCT dc.customer_id) AS active_customes
FROM dim_customers dc
JOIN fact_revenue fr
ON dc.customer_id = fr.customer_id
GROUP BY cohort_month, months_since_signup
ORDER BY cohort_month, months_since_signup;


WITH cohort_sizes AS (
    SELECT
        DATE_FORMAT(signup_date, '%Y-%m-01') AS cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM dim_customers
    GROUP BY cohort_month
)
SELECT
    c.cohort_month,
    TIMESTAMPDIFF(
        MONTH,
        DATE_FORMAT(dc.signup_date, '%Y-%m-01'),
        fr.revenue_date
    ) AS months_since_signup,
    COUNT(DISTINCT dc.customer_id) AS active_customers,
    ROUND(
        COUNT(DISTINCT dc.customer_id) * 1.0 / c.cohort_size,
        4
    ) AS retention_rate
FROM dim_customers dc
JOIN fact_revenue fr
  ON dc.customer_id = fr.customer_id
JOIN cohort_sizes c
  ON DATE_FORMAT(dc.signup_date, '%Y-%m-01') = c.cohort_month
GROUP BY c.cohort_month, months_since_signup
ORDER BY c.cohort_month, months_since_signup;


# USER-LEVEL METRICS
SELECT 
	customer_id,
    COUNT(DISTINCT revenue_date) AS active_months,
    SUM(amount) AS total_revenue,
    ROUND(AVG(amount),2) AS avg_monthly_revenue
FROM fact_revenue
GROUP BY customer_id;
## ORDER BY total_revenue DESC;


# Rank users by total revenue
WITH user_revenue AS(
	SELECT
		customer_id,
        SUM(amount) AS total_revenue
	FROM fact_revenue
    GROUP BY customer_id
),
ranked_users AS(
	SELECT
		customer_id,
        total_revenue,
        NTILE(5) OVER (ORDER BY total_revenue DESC) AS revenue_bucket
	FROM user_revenue
)
SELECT 
	MIN(total_revenue) AS revenue_80th_percentile
FROM ranked_users
WHERE revenue_bucket =1;


WITH user_metrics AS(
	SELECT 
		customer_id,
        COUNT(DISTINCT revenue_date) AS active_months,
        SUM(amount) AS total_revenue,
        ROUND(AVG(amount),2) AS avg_monthly_revenue
	FROM fact_revenue
    GROUP BY customer_id
),
revenue_cutoff AS(
	SELECT
		MIN(total_revenue) AS cutoff
	FROM(
		SELECT
			total_revenue,
            NTILE(5) OVER(ORDER BY total_revenue DESC) AS revenue_bucket
		FROM user_metrics
        )t
	WHERE revenue_bucket = 1
)
SELECT
	um.customer_id,
    um.active_months,
    um.total_revenue,
    um.avg_monthly_revenue,
    CASE 
		WHEN um.total_revenue >= rc.cutoff THEN 'Power User'
        ELSE 'Regular User'
	END AS user_segment
FROM user_metrics um
CROSS JOIN revenue_cutoff rc;


WITH user_revenue AS (
    SELECT
        customer_id,
        SUM(amount) AS total_revenue
    FROM fact_revenue
    GROUP BY customer_id
),
ranked_users AS (
    SELECT
        customer_id,
        total_revenue,
        NTILE(5) OVER (ORDER BY total_revenue DESC) AS revenue_bucket
    FROM user_revenue
),
cutoff AS (
    SELECT
        MIN(total_revenue) AS power_user_cutoff
    FROM ranked_users
    WHERE revenue_bucket = 1
)
SELECT
    CASE
        WHEN ur.total_revenue >= c.power_user_cutoff THEN 'Power User'
        ELSE 'Regular User'
    END AS user_segment,
    COUNT(*) AS users,
    ROUND(SUM(ur.total_revenue), 2) AS segment_revenue,
    ROUND(
        SUM(ur.total_revenue) / SUM(SUM(ur.total_revenue)) OVER (),
        4
    ) AS revenue_contribution_pct
FROM user_revenue ur
CROSS JOIN cutoff c
GROUP BY user_segment;

WITH user_metrics AS (
    SELECT
        customer_id,
        COUNT(DISTINCT revenue_date) AS active_months,
        SUM(amount) AS total_revenue,
        ROUND(AVG(amount), 2) AS avg_monthly_revenue
    FROM fact_revenue
    GROUP BY customer_id
),
ranked_users AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY total_revenue DESC) AS revenue_bucket
    FROM user_metrics
),
cutoff AS (
    SELECT
        MIN(total_revenue) AS power_user_cutoff
    FROM ranked_users
    WHERE revenue_bucket = 1
)
SELECT
    CASE
        WHEN um.total_revenue >= c.power_user_cutoff THEN 'Power User'
        ELSE 'Regular User'
    END AS user_segment,
    COUNT(*) AS users,
	ROUND(AVG(active_months), 2) AS avg_active_months,
    ROUND(AVG(avg_monthly_revenue), 2) AS avg_monthly_revenue,
    ROUND(AVG(total_revenue), 2) AS avg_lifetime_revenue
FROM user_metrics um
CROSS JOIN cutoff c
GROUP BY user_segment;

-- Plan Distribution
WITH user_revenue AS (
    SELECT
        customer_id,
        SUM(amount) AS total_revenue
    FROM fact_revenue
    GROUP BY customer_id
),
ranked_users AS (
    SELECT
        customer_id,
        total_revenue,
        NTILE(5) OVER (ORDER BY total_revenue DESC) AS revenue_bucket
    FROM user_revenue
),
cutoff AS (
    SELECT
        MIN(total_revenue) AS power_user_cutoff
    FROM ranked_users
    WHERE revenue_bucket = 1
),
user_segments AS (
    SELECT
        ur.customer_id,
        CASE
            WHEN ur.total_revenue >= c.power_user_cutoff THEN 'Power User'
            ELSE 'Regular User'
        END AS user_segment
    FROM user_revenue ur
    CROSS JOIN cutoff c
)
SELECT
    dc.plan_type,
    us.user_segment,
    COUNT(DISTINCT us.customer_id) AS users
FROM user_segments us
JOIN dim_customers dc
  ON us.customer_id = dc.customer_id
GROUP BY dc.plan_type, us.user_segment
ORDER BY dc.plan_type, us.user_segment;


-- livetime value by plan
SELECT 
	dc.plan_type,
    ROUND(AVG(ur.total_revenue),2) As avg_ltv
FROM(
	SELECT 
		customer_id,
        SUM(amount) AS total_revenue
	FROM fact_revenue
    GROUP BY customer_id
) ur
JOIN dim_customers dc
ON ur.customer_id = dc.customer_id
GROUP BY dc.plan_type
ORDER BY avg_ltv DESC;
