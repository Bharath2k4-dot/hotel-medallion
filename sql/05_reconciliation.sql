-- Reconciliation: revenue + booking counts consistency
-- Compare totals between silver.bookings and gold aggregates

-- 1) Revenue check (Confirmed revenue)
SELECT
  'confirmed_revenue' AS check_name,
  (SELECT ROUND(SUM(total_amount)::numeric,2) FROM silver.bookings WHERE booking_status='Confirmed') AS silver_value,
  (SELECT ROUND(SUM(revenue)::numeric,2) FROM gold.agg_monthly_kpis) AS gold_value,
  (SELECT ROUND(
    (SELECT SUM(total_amount)::numeric FROM silver.bookings WHERE booking_status='Confirmed')
    -
    (SELECT SUM(revenue)::numeric FROM gold.agg_monthly_kpis)
  ,2)) AS diff;

-- 2) Total bookings check
SELECT
  'total_bookings' AS check_name,
  (SELECT COUNT(*) FROM silver.bookings) AS silver_value,
  (SELECT SUM(total_bookings) FROM gold.agg_monthly_kpis) AS gold_value,
  (SELECT (SELECT COUNT(*) FROM silver.bookings) - (SELECT SUM(total_bookings) FROM gold.agg_monthly_kpis)) AS diff;
