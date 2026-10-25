-- Example analytical queries against the serving layer.

-- 1. Busiest pickup zones in the loaded period
SELECT z.zone, z.borough, SUM(m.trip_count) AS trips
FROM serving.hourly_zone_metrics m
LEFT JOIN serving.taxi_zones z ON z.location_id = m.pu_location_id
GROUP BY z.zone, z.borough
ORDER BY trips DESC
LIMIT 10;

-- 2. Demand profile by hour of day (all zones)
SELECT EXTRACT(HOUR FROM pickup_hour)::int AS hour_of_day,
       SUM(trip_count)                      AS trips,
       SUM(sum_fare_amount) / SUM(trip_count) AS avg_fare
FROM serving.hourly_zone_metrics
GROUP BY 1
ORDER BY 1;

-- 3. Weekday vs weekend average demand per zone-hour
SELECT f.is_weekend, AVG(f.target_trip_count) AS avg_trips_per_zone_hour
FROM serving.forecast_features f
GROUP BY f.is_weekend;

-- 4. Time series for one zone (ML access pattern; uses idx_ff_zone_hour)
SELECT pickup_hour, target_trip_count, lag_24h_trip_count, rolling_mean_24h_trip_count
FROM serving.forecast_features
WHERE pu_location_id = 161
  AND pickup_hour >= TIMESTAMP '2024-01-08' AND pickup_hour < TIMESTAMP '2024-01-15'
ORDER BY pickup_hour;

-- 5. Data-quality summary of every batch (source = accepted + rejected)
SELECT batch_id, source_rows, accepted_rows, rejected_rows,
       ROUND(100.0 * rejected_rows / NULLIF(source_rows, 0), 3) AS rejected_pct,
       reject_breakdown
FROM ops.batch_runs
ORDER BY finished_at DESC;

-- 6. Reconciliation: aggregated trips must equal accepted rows for the batch
SELECT b.batch_id,
       b.accepted_rows,
       (SELECT SUM(trip_count) FROM serving.hourly_zone_metrics m
         WHERE m.pickup_hour >= b.window_start AND m.pickup_hour < b.window_end) AS aggregated_trips
FROM ops.batch_runs b;
