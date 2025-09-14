-- Ensure target schema exists
CREATE SCHEMA IF NOT EXISTS marts;

-- 1) Trips per hour
CREATE OR REPLACE VIEW marts.v_trips_per_hour AS
SELECT hour, SUM(trips) AS trips
FROM marts.fct_trip_metrics
GROUP BY hour;

-- 2) Revenue per hour
CREATE OR REPLACE VIEW marts.v_revenue_per_hour AS
SELECT hour, SUM(revenue) AS revenue
FROM marts.fct_trip_metrics
GROUP BY hour;

-- 3) Average fare & tip per hour
CREATE OR REPLACE VIEW marts.v_avg_fare_tip_per_hour AS
SELECT hour,
       AVG(avg_fare) AS avg_fare,
       AVG(avg_tip)  AS avg_tip
FROM marts.fct_trip_metrics
GROUP BY hour;

-- 4) Tip % by hour (from raw)
CREATE OR REPLACE VIEW marts.v_tip_pct_by_hour AS
SELECT date_trunc('hour', tpep_pickup_datetime) AS hour,
       SUM(tip_amount) / NULLIF(SUM(fare_amount), 0) * 100 AS tip_pct
FROM staging.yellow_trips
GROUP BY 1;

-- 5) Payment mix
CREATE OR REPLACE VIEW marts.v_payment_mix AS
SELECT payment_type,
       COUNT(*) AS trips,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_trips
FROM staging.yellow_trips
GROUP BY payment_type;

-- 6) Top pickup zones by revenue (no LIMIT; filter Top N in BI)
CREATE OR REPLACE VIEW marts.v_pickup_zones_by_revenue AS
SELECT pulocationid AS pickup_zone,
       SUM(total_amount) AS revenue,
       COUNT(*) AS trips
FROM staging.yellow_trips
GROUP BY pulocationid;

-- 7) Fare & distance distribution summary
CREATE OR REPLACE VIEW marts.v_fare_distance_summary AS
SELECT
  AVG(trip_distance) AS avg_miles,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trip_distance) AS median_miles,
  AVG(fare_amount) AS avg_fare,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fare_amount)  AS median_fare
FROM staging.yellow_trips;

-- 8) Peak hours (expose all; apply Top 10 in BI)
CREATE OR REPLACE VIEW marts.v_peak_hours AS
SELECT hour, trips, revenue
FROM marts.fct_trip_metrics;

-- 9) Weekday vs Weekend trips
CREATE OR REPLACE VIEW marts.v_weekday_weekend_trips AS
SELECT CASE WHEN EXTRACT(DOW FROM tpep_pickup_datetime) IN (0,6)
            THEN 'Weekend' ELSE 'Weekday' END AS day_type,
       COUNT(*) AS trips
FROM staging.yellow_trips
GROUP BY 1;

-- 10) Trips per day of week
CREATE OR REPLACE VIEW marts.v_trips_by_dow AS
SELECT EXTRACT(DOW FROM tpep_pickup_datetime)         AS dow_num,
       TRIM(TO_CHAR(tpep_pickup_datetime,'Day'))      AS day_name,
       COUNT(*)                                       AS trips
FROM staging.yellow_trips
GROUP BY 1,2;

-- 11) Distance buckets (trips share)
CREATE OR REPLACE VIEW marts.v_distance_buckets AS
SELECT
  CASE
    WHEN trip_distance < 1  THEN '0–1 mile'
    WHEN trip_distance < 3  THEN '1–3 miles'
    WHEN trip_distance < 5  THEN '3–5 miles'
    WHEN trip_distance < 10 THEN '5–10 miles'
    ELSE '10+ miles'
  END AS distance_bucket,
  COUNT(*) AS trips,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_trips
FROM staging.yellow_trips
GROUP BY 1;

-- 12) Top drop-off zones (expose all)
CREATE OR REPLACE VIEW marts.v_dropoff_zones_by_trips AS
SELECT dolocationid AS dropoff_zone,
       COUNT(*)       AS trips
FROM staging.yellow_trips
GROUP BY dolocationid;

-- 13) Hourly average speed (mph)
CREATE OR REPLACE VIEW marts.v_hourly_avg_speed AS
SELECT
  date_trunc('hour', tpep_pickup_datetime) AS hour,
  AVG(
    trip_distance /
    NULLIF(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0, 0)
  ) AS avg_speed_mph
FROM staging.yellow_trips
GROUP BY 1;

-- 14) High-fare outliers (expose all; filter in BI)
CREATE OR REPLACE VIEW marts.v_high_fare_outliers AS
SELECT
  trip_distance, fare_amount, total_amount, passenger_count, tpep_pickup_datetime
FROM staging.yellow_trips
WHERE fare_amount > 200;

-- 15) Revenue by distance bucket
CREATE OR REPLACE VIEW marts.v_revenue_by_distance_bucket AS
SELECT
  CASE
    WHEN trip_distance < 1  THEN '0–1 mile'
    WHEN trip_distance < 3  THEN '1–3 miles'
    WHEN trip_distance < 5  THEN '3–5 miles'
    WHEN trip_distance < 10 THEN '5–10 miles'
    ELSE '10+ miles'
  END AS distance_bucket,
  SUM(total_amount) AS revenue
FROM staging.yellow_trips
GROUP BY 1;

-- 16) Average fare by passenger count
CREATE OR REPLACE VIEW marts.v_avg_fare_by_passenger_count AS
SELECT passenger_count,
       AVG(fare_amount) AS avg_fare
FROM staging.yellow_trips
WHERE passenger_count IS NOT NULL AND passenger_count > 0
GROUP BY passenger_count
ORDER BY passenger_count;
