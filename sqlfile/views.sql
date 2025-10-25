-- 0) Khu vực dùng cho view
CREATE SCHEMA IF NOT EXISTS dw_mart;

-- 1) View nền: join đủ thông tin đơn hàng + nhà hàng + thời gian
CREATE OR REPLACE VIEW dw_mart.v_orders_base AS
SELECT
  f.order_id,
  f.order_placed_at,
  (f.order_placed_at)::date AS order_date,
  f.order_status,
  f.delivery_type,
  -- distance/items_in_order đang là TEXT, parse nhẹ ra số để vẽ biểu đồ
  NULLIF(regexp_replace(lower(f.distance), '[^0-9\.]', '', 'g'), '')::numeric AS distance_km,
  CASE
    WHEN f.items_in_order IS NULL THEN 0
    ELSE (length(f.items_in_order) - length(replace(f.items_in_order, ' x ', ''))) / 3
  END::int AS items_count,
  f.bill_subtotal,
  f.packaging_charges,
  f.restaurant_discount_promo,
  f.restaurant_discount_flat,
  f.gold_discount,
  f.brand_pack_discount,
  f.total,
  r.restaurant_id,
  r.restaurant_name,
  r.subzone,
  r.city
FROM dw.fact_orders f
JOIN dw.dim_restaurant r ON r.restaurant_id = f.restaurant_id;

-- 2) Doanh thu theo ngày
CREATE OR REPLACE VIEW dw_mart.v_revenue_daily AS
SELECT
  order_date,
  COUNT(*)               AS orders,
  SUM(total)             AS revenue,
  AVG(total)             AS aov
FROM dw_mart.v_orders_base
GROUP BY order_date
ORDER BY order_date;

-- 3) Doanh thu theo City/Subzone (map heatmap)
CREATE OR REPLACE VIEW dw_mart.v_revenue_by_area AS
SELECT
  city, subzone,
  COUNT(*)   AS orders,
  SUM(total) AS revenue
FROM dw_mart.v_orders_base
GROUP BY city, subzone
ORDER BY revenue DESC;

-- 4) Tỉ lệ trạng thái đơn hàng
CREATE OR REPLACE VIEW dw_mart.v_order_status_ratio AS
SELECT
  order_status,
  COUNT(*)                              AS orders,
  ROUND(100.0 * COUNT(*)/SUM(COUNT(*)) OVER (), 2) AS pct
FROM dw_mart.v_orders_base
GROUP BY order_status
ORDER BY orders DESC;

-- 5) Phân phối distance & số món (đã chuẩn hoá để vẽ histogram)
CREATE OR REPLACE VIEW dw_mart.v_distance_items_dist AS
SELECT
  order_date,
  distance_km,
  items_count,
  total
FROM dw_mart.v_orders_base
WHERE distance_km IS NOT NULL;

-- 6) Event funnel (app events)
CREATE OR REPLACE VIEW dw_mart.v_event_funnel AS
SELECT
  event_name,
  (timestamp::date) AS event_date,
  COUNT(*)          AS events
FROM dw.fact_app_events
GROUP BY event_name, (timestamp::date)
ORDER BY event_date, event_name;

-- 7) Reviews: điểm & sentiment
CREATE OR REPLACE VIEW dw_mart.v_reviews_summary AS
SELECT
  score,
  COUNT(*)               AS reviews,
  AVG(sentiment_score)   AS avg_sentiment
FROM dw.fact_reviews
GROUP BY score
ORDER BY score;

-- 8) Top nhà hàng theo doanh thu / số đơn
CREATE OR REPLACE VIEW dw_mart.v_top_restaurants AS
SELECT
  restaurant_id,
  restaurant_name,
  city, subzone,
  COUNT(*)   AS orders,
  SUM(total) AS revenue
FROM dw_mart.v_orders_base
GROUP BY restaurant_id, restaurant_name, city, subzone
ORDER BY revenue DESC;

SELECT 'v_orders_base'        AS view, COUNT(*) FROM dw_mart.v_orders_base
UNION ALL SELECT 'v_revenue_daily',       COUNT(*) FROM dw_mart.v_revenue_daily
UNION ALL SELECT 'v_revenue_by_area',     COUNT(*) FROM dw_mart.v_revenue_by_area
UNION ALL SELECT 'v_order_status_ratio',  COUNT(*) FROM dw_mart.v_order_status_ratio
UNION ALL SELECT 'v_distance_items_dist', COUNT(*) FROM dw_mart.v_distance_items_dist
UNION ALL SELECT 'v_event_funnel',        COUNT(*) FROM dw_mart.v_event_funnel
UNION ALL SELECT 'v_reviews_summary',     COUNT(*) FROM dw_mart.v_reviews_summary
UNION ALL SELECT 'v_top_restaurants',     COUNT(*) FROM dw_mart.v_top_restaurants;

-- Đơn hàng nền (đầy đủ cột để visualize)
SELECT * FROM dw_mart.v_orders_base ORDER BY order_placed_at DESC LIMIT 20;

-- Doanh thu theo ngày
SELECT * FROM dw_mart.v_revenue_daily ORDER BY order_date DESC LIMIT 20;

-- Doanh thu theo khu vực
SELECT * FROM dw_mart.v_revenue_by_area ORDER BY revenue DESC LIMIT 20;

-- Tỉ lệ trạng thái đơn
SELECT * FROM dw_mart.v_order_status_ratio ORDER BY orders DESC;

-- Phân phối khoảng cách & số món (để vẽ histogram)
SELECT * FROM dw_mart.v_distance_items_dist ORDER BY order_date DESC LIMIT 20;

-- Funnel sự kiện app
SELECT * FROM dw_mart.v_event_funnel ORDER BY event_date DESC, event_name LIMIT 50;

-- Tổng quan review
SELECT * FROM dw_mart.v_reviews_summary ORDER BY score;

-- Top nhà hàng
SELECT * FROM dw_mart.v_top_restaurants ORDER BY revenue DESC LIMIT 20;

