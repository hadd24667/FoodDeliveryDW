-- ==============================
-- DIMENSIONS
-- ==============================

CREATE TABLE dw.dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    age INT,
    gender VARCHAR(10),
    marital_status VARCHAR(20),
    occupation VARCHAR(50),
    education VARCHAR(50),
    family_size INT,
    medium_used VARCHAR(50),
    meal_category VARCHAR(50),
    preference VARCHAR(50),
    restaurant_rating INT,
    delivery_rating INT,
    orders_placed INT,
    delivery_time INT,
    order_value INT,
    ease_convenience INT,
    self_cooking BOOLEAN,
    health_concern INT,
    late_delivery BOOLEAN,
    poor_hygiene BOOLEAN,
    bad_experience INT,
    more_offers_discount INT,
    max_wait_time VARCHAR(50),
    influence_of_rating VARCHAR(50)
);

CREATE TABLE dw.dim_restaurant (
    restaurant_id VARCHAR(100) PRIMARY KEY,
    restaurant_name VARCHAR(200),
    subzone VARCHAR(100),
    city VARCHAR(100)
);

CREATE TABLE dw.dim_customer_orders (
    customer_id VARCHAR(100) PRIMARY KEY
);

CREATE TABLE dw.dim_time (
    time_key SERIAL PRIMARY KEY,
    order_placed_at TIMESTAMP,
    date DATE,
    time TIME,
    day INT,
    month INT,
    year INT,
    weekday VARCHAR(20)
);

CREATE TABLE dw.dim_reviewer (
    reviewer_id VARCHAR(50) PRIMARY KEY
);

CREATE TABLE dw.dim_user (
    user_sk VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50)
);

-- ==============================
-- FACTS
-- ==============================

CREATE TABLE dw.fact_orders (
    order_id VARCHAR(100) PRIMARY KEY,
    restaurant_id VARCHAR(50) REFERENCES dw.dim_restaurant(restaurant_id),
    customer_id VARCHAR(100) REFERENCES dw.dim_customer_orders(customer_id),
    order_placed_at TIMESTAMP,
    order_status VARCHAR(50),
    delivery_type VARCHAR(50),
    distance TEXT,
    items_in_order TEXT,
    instructions TEXT,
    discount_construct TEXT,
    bill_subtotal DECIMAL(10,2),
    packaging_charges DECIMAL(10,2),
    restaurant_discount_promo DECIMAL(10,2),
    restaurant_discount_flat DECIMAL(10,2),
    gold_discount DECIMAL(10,2),
    brand_pack_discount DECIMAL(10,2),
    total DECIMAL(10,2),
    cancellation_reason TEXT,
    restaurant_compensation DECIMAL(10,2),
    restaurant_penalty DECIMAL(10,2),
    kpt_duration DECIMAL(10,2),
    rider_wait_time DECIMAL(10,2),
    order_ready_marked VARCHAR(50)
);

CREATE TABLE dw.fact_reviews (
    reviewer_id VARCHAR(50) REFERENCES dw.dim_reviewer(reviewer_id),
    score INT,
    time VARCHAR(50),
    summary TEXT,
    sentiment_score FLOAT
);

CREATE TABLE dw.fact_app_events (
    user_sk VARCHAR(50) REFERENCES dw.dim_user(user_sk),
    sessionid INTEGER,
    timestamp VARCHAR(50),
    event_name VARCHAR(100),
    productid VARCHAR(100),
    amount DECIMAL(10,2),
    outcome VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS ix_orders_time ON dw.fact_orders(order_placed_at);
CREATE INDEX IF NOT EXISTS ix_orders_restaurant ON dw.fact_orders(restaurant_id);
CREATE INDEX IF NOT EXISTS ix_orders_customer ON dw.fact_orders(customer_id);