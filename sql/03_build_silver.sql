CREATE SCHEMA IF NOT EXISTS silver;

-- ======================
-- SILVER: HOTELS
-- ======================
DROP TABLE IF EXISTS silver.hotels;
CREATE TABLE silver.hotels AS
SELECT
  hotel_id,
  hotel_name,
  city,
  total_rooms::int
FROM bronze.hotels_raw
WHERE hotel_id IS NOT NULL
  AND total_rooms::int > 0;

INSERT INTO audit.rejected_rows
(run_id, layer, source_table, target_table, reason, row_data)
SELECT
  :'run_id',
  'silver',
  'bronze.hotels_raw',
  'silver.hotels',
  'INVALID_HOTEL',
  to_jsonb(h)
FROM bronze.hotels_raw h
WHERE h.hotel_id IS NULL OR h.total_rooms::int <= 0;

-- ======================
-- SILVER: ROOM TYPES
-- ======================
DROP TABLE IF EXISTS silver.room_types;
CREATE TABLE silver.room_types AS
SELECT
  room_type_id,
  hotel_id,
  room_type,
  base_rate::int AS base_rate
FROM bronze.room_types_raw
WHERE room_type_id IS NOT NULL
  AND base_rate::int > 0;

-- ======================
-- SILVER: BOOKINGS
-- ======================
DROP TABLE IF EXISTS silver.bookings;
CREATE TABLE silver.bookings AS
SELECT
  booking_id,
  hotel_id,
  room_type,
  to_date(booking_date, 'MM/DD/YYYY') AS booking_date,
  to_date(checkin_date, 'MM/DD/YYYY') AS checkin_date,
  to_date(checkout_date, 'MM/DD/YYYY') AS checkout_date,
  nights::int,
  booking_channel,
  market_segment,
  booking_status,
  room_price_per_night::int,
  total_amount::int,
  city
FROM bronze.bookings_raw
WHERE booking_id IS NOT NULL;

-- ======================
-- SILVER: PAYMENTS
-- ======================
DROP TABLE IF EXISTS silver.payments;
CREATE TABLE silver.payments AS
SELECT
  payment_id,
  booking_id,
  payment_status,
  paid_amount::int
FROM bronze.payments_raw
WHERE payment_id IS NOT NULL
  AND paid_amount::int >= 0;

-- ======================
-- SILVER: ROOM INVENTORY
-- ======================
DROP TABLE IF EXISTS silver.room_inventory;
CREATE TABLE silver.room_inventory AS
SELECT
  hotel_id,
  to_date(date, 'MM/DD/YYYY') AS date,
  available_rooms::int,
  occupied_rooms::int
FROM bronze.room_inventory_raw
WHERE hotel_id IS NOT NULL;
