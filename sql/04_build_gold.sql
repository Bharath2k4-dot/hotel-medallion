CREATE SCHEMA IF NOT EXISTS gold;

-- =========================================================
-- GOLD 1) agg_monthly_kpis
-- =========================================================
DROP TABLE IF EXISTS gold.agg_monthly_kpis;
CREATE TABLE gold.agg_monthly_kpis AS
WITH b AS (
  SELECT
    date_trunc('month', checkin_date)::date AS month,
    booking_status,
    nights,
    total_amount,
    room_price_per_night
  FROM silver.bookings
),
inventory AS (
  SELECT
    date_trunc('month', date)::date AS month,
    SUM(available_rooms) AS available_room_nights,
    SUM(occupied_rooms) AS occupied_room_nights
  FROM silver.room_inventory
  GROUP BY 1
),
bookings AS (
  SELECT
    month,
    COUNT(*) AS total_bookings,
    SUM(CASE WHEN booking_status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_bookings,
    SUM(CASE WHEN booking_status = 'Confirmed' THEN total_amount ELSE 0 END) AS confirmed_revenue,
    AVG(CASE WHEN booking_status = 'Confirmed' THEN room_price_per_night END) AS adr
  FROM b
  GROUP BY 1
)
SELECT
  bk.month,
  bk.total_bookings,
  bk.cancelled_bookings,
  ROUND((bk.cancelled_bookings::numeric / NULLIF(bk.total_bookings,0)) * 100, 2) AS cancellation_rate_pct,
  ROUND(bk.confirmed_revenue::numeric, 2) AS revenue,
  ROUND(bk.adr::numeric, 2) AS adr,
  inv.available_room_nights,
  inv.occupied_room_nights,
  ROUND((inv.occupied_room_nights::numeric / NULLIF(inv.available_room_nights,0)) * 100, 2) AS occupancy_rate_pct,
  ROUND((bk.confirmed_revenue::numeric / NULLIF(inv.available_room_nights,0)), 2) AS revpar
FROM bookings bk
LEFT JOIN inventory inv USING (month)
ORDER BY bk.month;

-- =========================================================
-- GOLD 2) agg_channel_segment_monthly
-- =========================================================
DROP TABLE IF EXISTS gold.agg_channel_segment_monthly;
CREATE TABLE gold.agg_channel_segment_monthly AS
SELECT
  date_trunc('month', checkin_date)::date AS month,
  booking_channel,
  market_segment,
  COUNT(*) AS total_bookings,
  SUM(CASE WHEN booking_status='Cancelled' THEN 1 ELSE 0 END) AS cancelled_bookings,
  ROUND(SUM(CASE WHEN booking_status='Confirmed' THEN total_amount ELSE 0 END)::numeric, 2) AS revenue,
  ROUND(AVG(CASE WHEN booking_status='Confirmed' THEN room_price_per_night END)::numeric, 2) AS adr
FROM silver.bookings
GROUP BY 1,2,3
ORDER BY 1,2,3;

-- =========================================================
-- GOLD 3) dashboard_booking_fact (wide table for Looker Studio)
-- =========================================================
DROP TABLE IF EXISTS gold.dashboard_booking_fact;
CREATE TABLE gold.dashboard_booking_fact AS
WITH pay AS (
  SELECT
    booking_id,
    MAX(payment_status) AS payment_status,
    SUM(paid_amount) AS paid_amount
  FROM silver.payments
  GROUP BY 1
)
SELECT
  b.booking_id,
  b.hotel_id,
  h.hotel_name,
  h.city,
  b.room_type,
  b.booking_date,
  b.checkin_date,
  b.checkout_date,
  b.nights,
  b.booking_channel,
  b.market_segment,
  b.booking_status,
  b.room_price_per_night,
  b.total_amount,
  COALESCE(p.payment_status, 'Unknown') AS payment_status,
  COALESCE(p.paid_amount, 0) AS paid_amount,
  date_trunc('month', b.checkin_date)::date AS month,
  i.available_rooms,
  i.occupied_rooms
FROM silver.bookings b
JOIN silver.hotels h ON h.hotel_id = b.hotel_id
LEFT JOIN pay p ON p.booking_id = b.booking_id
LEFT JOIN silver.room_inventory i
  ON i.hotel_id = b.hotel_id
 AND i.date = b.checkin_date;
