CREATE TABLE fact_reviews (
    review_id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    score INT,
    time BIGINT,
    summary TEXT,
    sentiment_score FLOAT
);
