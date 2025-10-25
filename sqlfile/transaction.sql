-- 1. Restaurant Dimension
CREATE TABLE dim_restaurant (
    restaurant_id BIGINT PRIMARY KEY,
    restaurant_name VARCHAR(255),
    subzone VARCHAR(100),
    city VARCHAR(100)
);

-- 2. Customer Dimension
CREATE TABLE dim_customer_orders (
    customer_id VARCHAR(100) PRIMARY KEY
);

-- 3. Time Dimension
CREATE TABLE dim_time (
    time_key SERIAL PRIMARY KEY,
    order_placed_at TIMESTAMP,
    date DATE,
    time TIME,
    day INT,
    month INT,
    year INT,
    weekday VARCHAR(20)
);

-- ==========================
-- FACT TABLE
-- ==========================

CREATE TABLE fact_orders (
    order_id BIGINT PRIMARY KEY,
    restaurant_id BIGINT REFERENCES dim_restaurant(restaurant_id),
    customer_id VARCHAR(100) REFERENCES dim_customer_orders(customer_id),
    order_placed_at TIMESTAMP,
    order_status VARCHAR(50),
    delivery_type VARCHAR(50),
    distance VARCHAR(20),
    items_in_order TEXT,
    instructions TEXT,
    discount_construct VARCHAR(100),
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
    kpt_duration DECIMAL(5,2),
    rider_wait_time DECIMAL(5,2),
    order_ready_marked VARCHAR(50)
);

-- ==========================
-- INDEXES
-- ==========================

CREATE INDEX idx_fact_orders_restaurant_id ON fact_orders(restaurant_id);
CREATE INDEX idx_fact_orders_customer_id ON fact_orders(customer_id);
CREATE INDEX idx_fact_orders_order_status ON fact_orders(order_status);