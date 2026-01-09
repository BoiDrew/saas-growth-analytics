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


-- User Month revenue timeline
WITH user_month_revenue AS (
	SELECT 
		dc.customer_id,
        DATE_FORMAT(dc.signup_date, '%Y-%m-01') AS cohort_month,
        TIMESTAMPDIFF(
			MONTH,
            DATE_FORMAT(dc.signup_date, '%Y-%m-01'),
            fr.revenue_date
		)AS months_since_signup,
        SUM(fr.amount) AS monthly_revenue
	FROM dim_customers dc
    JOIN fact_revenue fr
    ON dc.customer_id = fr.customer_id
    GROUP BY dc.customer_id, cohort_month, months_since_signup
)
SELECT *
FROM user_month_revenue
ORDER BY customer_id, months_since_signup;

-- Assign Segments
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
SELECT * FROM user_segments;

-- Cumulative LTV by Segment
WITH user_month_revenue AS (
    SELECT
        dc.customer_id,
        TIMESTAMPDIFF(
            MONTH,
            DATE_FORMAT(dc.signup_date, '%Y-%m-01'),
            fr.revenue_date
        ) AS months_since_signup,
        SUM(fr.amount) AS monthly_revenue
    FROM dim_customers dc
    JOIN fact_revenue fr
      ON dc.customer_id = fr.customer_id
    GROUP BY dc.customer_id, months_since_signup
),
user_revenue AS (
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
),
cumulative_ltv AS (
    SELECT
        umr.customer_id,
        us.user_segment,
        umr.months_since_signup,
        SUM(umr.monthly_revenue)
            OVER (
                PARTITION BY umr.customer_id
                ORDER BY umr.months_since_signup
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_revenue
    FROM user_month_revenue umr
    JOIN user_segments us
      ON umr.customer_id = us.customer_id
)
SELECT
    user_segment,
    months_since_signup,
    ROUND(AVG(cumulative_revenue), 2) AS avg_ltv
FROM cumulative_ltv
GROUP BY user_segment, months_since_signup
ORDER BY user_segment, months_since_signup;

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
),
early_revenue AS (
    SELECT
        customer_id,
        SUM(amount) AS revenue_first_2_months
    FROM fact_revenue
    WHERE revenue_date <= DATE_ADD(
        (SELECT MIN(revenue_date) FROM fact_revenue),
        INTERVAL 2 MONTH
    )
    GROUP BY customer_id
)
SELECT
    us.user_segment,
    ROUND(AVG(er.revenue_first_2_months), 2) AS avg_early_revenue
FROM early_revenue er
JOIN user_segments us
  ON er.customer_id = us.customer_id
GROUP BY us.user_segment;


-- Revenue by Plan(where money comes from)
SELECT 
	dc.plan_type,
    ROUND(SUM(fr.amount),2) AS total_revenue,
    ROUND(AVG(fr.amount),2) AS avg_monthly_revenue
FROM fact_revenue fr
JOIN dim_customers dc
ON fr.customer_id = dc.customer_id
GROUP BY dc.plan_type
ORDER BY total_revenue DESC;



