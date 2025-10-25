DROP TABLE IF EXISTS public.fact_app_events CASCADE;
DROP TABLE IF EXISTS public.dim_user CASCADE;

CREATE TABLE public.dim_user (
    user_sk VARCHAR(50) PRIMARY KEY,
    userid INTEGER NOT NULL
);

CREATE TABLE public.fact_app_events (
    user_sk VARCHAR(50) REFERENCES dim_user(user_sk),
    sessionid INTEGER,
    timestamp VARCHAR(50),
    event_name VARCHAR(100),
    productid VARCHAR(100),
    amount DECIMAL(10,2),
    outcome VARCHAR(100)
);

select * from dim_user;
select * from fact_app_events;
