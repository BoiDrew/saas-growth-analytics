USE saas_analytics; 

CREATE TABLE fact_revenue (
	revenue_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    subscription_id INT,
    revenue_date DATE,
    revenue_type varchar(50),
    monthly_fee DECIMAL(10,2),
    amount DECIMAL(10,2),
    
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (subscription_id) REFERENCES dim_subscriptions(subscription_id)
);
