CREATE SCHEMA IF NOT EXISTS silver;

-- ============
-- SILVER: HOTELS
-- ============
DROP TABLE IF EXISTS silver.hotels;
CREATE TABLE silver.hotels AS
SELECT
  hotel_id,
  hotel_name,
  city,
  total_rooms
FROM bronze.hotels_raw
WHERE hotel_id IS NOT NULL
  AND total_rooms > 0;

INSERT INTO audit.rejected_rows (run_id, layer, source_table, target_table, reason, row_data)
SELECT
  :run_id,
  'silver',
  'bronze.hotels_raw',
  'silver.hotels',
  'INVALID_HOTEL',
  to_jsonb(h)
FROM bronze.hotels_raw h
WHERE h.hotel_id IS NULL OR h.total_rooms <= 0;

-- ============
-- SILVER: ROOM TYPES
-- ============
DROP TABLE IF EXISTS silver.room_types;
CREATE TABLE silver.room_types AS
SELECT
  room_type_id,
  hotel_id,
  room_type,
  base_rate
FROM bronze.room_types_raw
WHERE room_type_id IS NOT NULL
  AND base_rate > 0;

INSERT INTO audit.rejected_rows
SELECT
  :run_id,
  'silver',
  'bronze.room_types_raw',
  'silver.room_types',
  'INVALID_ROOM_TYPE',
  to_jsonb(r)
FROM bronze.room_types_raw r
LEFT JOIN silver.hotels h ON h.hotel_id = r.hotel_id
WHERE r.room_type_id IS NULL
   OR r.base_rate <= 0
   OR h.hotel_id IS NULL;

-- ============
-- SILVER: BOOKINGS
-- ============
DROP TABLE IF EXISTS silver.bookings;
CREATE TABLE silver.bookings AS
SELECT
  booking_id,
  hotel_id,
  room_type,
  booking_date,
  checkin_date,
  checkout_date,
  nights,
  booking_channel,
  market_segment,
  booking_status,
  room_price_per_night,
  total_amount,
  city
FROM bronze.bookings_raw
WHERE booking_id IS NOT NULL
  AND nights > 0
  AND total_amount >= 0
  AND booking_status IN ('Confirmed','Cancelled');

INSERT INTO audit.rejected_rows
SELECT
  :run_id,
  'silver',
  'bronze.bookings_raw',
  'silver.bookings',
  'INVALID_BOOKING',
  to_jsonb(b)
FROM bronze.bookings_raw b
LEFT JOIN silver.hotels h ON h.hotel_id = b.hotel_id
WHERE b.booking_id IS NULL
   OR b.nights <= 0
   OR b.total_amount < 0
   OR b.booking_status NOT IN ('Confirmed','Cancelled')
   OR h.hotel_id IS NULL;

-- ============
-- SILVER: PAYMENTS
-- ============
DROP TABLE IF EXISTS silver.payments;
CREATE TABLE silver.payments AS
SELECT
  payment_id,
  booking_id,
  payment_status,
  paid_amount
FROM bronze.payments_raw
WHERE payment_status IN ('Paid','Refunded')
  AND paid_amount >= 0;

INSERT INTO audit.rejected_rows
SELECT
  :run_id,
  'silver',
  'bronze.payments_raw',
  'silver.payments',
  'INVALID_PAYMENT',
  to_jsonb(p)
FROM bronze.payments_raw p
LEFT JOIN silver.bookings b ON b.booking_id = p.booking_id
WHERE p.payment_status NOT IN ('Paid','Refunded')
   OR p.paid_amount < 0
   OR b.booking_id IS NULL;

-- ============
-- SILVER: ROOM INVENTORY
-- ============
DROP TABLE IF EXISTS silver.room_inventory;
CREATE TABLE silver.room_inventory AS
SELECT
  hotel_id,
  date,
  available_rooms,
  occupied_rooms
FROM bronze.room_inventory_raw
WHERE available_rooms > 0
  AND occupied_rooms BETWEEN 0 AND available_rooms;

INSERT INTO audit.rejected_rows
SELECT
  :run_id,
  'silver',
  'bronze.room_inventory_raw',
  'silver.room_inventory',
  'INVALID_INVENTORY',
  to_jsonb(i)
FROM bronze.room_inventory_raw i
LEFT JOIN silver.hotels h ON h.hotel_id = i.hotel_id
WHERE i.available_rooms <= 0
   OR i.occupied_rooms < 0
   OR i.occupied_rooms > i.available_rooms
   OR h.hotel_id IS NULL;
