-- Table: public.dim_customer

-- DROP TABLE IF EXISTS public.dim_customer;

CREATE TABLE IF NOT EXISTS public.dim_customer
(
    customer_id text COLLATE pg_catalog."default",
    age bigint,
    gender text COLLATE pg_catalog."default",
    marital_status text COLLATE pg_catalog."default",
    occupation text COLLATE pg_catalog."default",
    education text COLLATE pg_catalog."default",
    family_size bigint,
    medium_used text COLLATE pg_catalog."default",
    meal_category text COLLATE pg_catalog."default",
    preference text COLLATE pg_catalog."default",
    restaurant_rating bigint,
    delivery_rating bigint,
    orders_placed bigint,
    delivery_time text COLLATE pg_catalog."default",
    order_value bigint,
    ease_convenience double precision,
    self_cooking bigint,
    health_concern double precision,
    late_delivery bigint,
    poor_hygiene bigint,
    bad_experience double precision,
    more_offers_discount double precision,
    max_wait_time text COLLATE pg_catalog."default",
    influence_of_rating text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dim_customer
    OWNER to admin;