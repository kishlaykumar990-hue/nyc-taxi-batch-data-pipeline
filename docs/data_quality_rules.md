# Data-quality rules, metrics and timestamp policy

Source: NYC TLC Yellow Taxi trip records (official Parquet). TLC states the data
comes from technology providers and that TLC "makes no representations as to the
accuracy of these data" - hence explicit, auditable rules.

## Row outcomes
Every source row is **accepted** or **rejected with exactly one reason**; nothing
is silently dropped. Invariants enforced in code and DB: `source = accepted + rejected`
and `sum(trip_count) = accepted`. Rejected rows are kept in `taxi-rejected` with `reject_reason`.

| Order | Reason | Rule | Rationale |
|---|---|---|---|
| 1 | null_pickup_ts / null_dropoff_ts | timestamp missing/unparseable | cannot be placed in time |
| 2 | pickup_outside_window | pickup not in [month start, next month start) | TLC files contain stray dates (e.g. 2002) |
| 3 | non_positive_duration | dropoff <= pickup | impossible |
| 4 | duration_exceeds_max | > `MAX_TRIP_HOURS` (24) | implausible/meter left running |
| 5 | null_pickup_zone | PULocationID null | cannot be aggregated by zone |
| 6-8 | null_fare / negative_fare / fare_exceeds_max | fare_amount null, <0, > `MAX_FARE_AMOUNT` (1000) | invalid; refunds/disputes are not demand |
| 9-11 | null/negative/exceeds distance | trip_distance null, <0, > `MAX_TRIP_DISTANCE_MILES` (500) | invalid |
| 12 | duplicate | exact repeat of an otherwise valid row (9 key fields); first kept | double submission |

**Deliberately kept (not assumed invalid):** zero distance, zero fare, null passenger
count, and unknown zones 264/265 (valid TLC codes). The limits are configurable in
`.env`; they are defensible defaults, not TLC rules. Rule choices should be reviewed
against the TLC data dictionary before drawing business conclusions.

## Timestamp policy
TLC timestamps are NYC **local wall-clock time without offset**. The pipeline never
converts them: Spark runs in UTC only so the naive values round-trip unchanged;
Postgres columns are `TIMESTAMP` (without time zone); window bounds are passed as
strings. Consequence: the DST fall-back hour is ambiguous in the source and the
spring-forward hour has no trips; both are inherent to the data and documented here.

## Metrics
| Column | Meaning / limitation |
|---|---|
| trip_count | accepted trips per (pickup hour, pickup zone) |
| avg_fare, p50_fare, p90_fare | on `fare_amount` = metered fare only (no tip, tolls, surcharges, taxes); percentiles approximate (`percentile_approx`) |
| sum_fare_amount | sum of `fare_amount` |
| sum_total_amount | sum of `total_amount` = all charges **except cash tips** |
| avg_trip_distance | miles, taximeter-reported |
| avg_duration_min | dropoff - pickup |
| avg_passenger_count | driver-entered, nulls ignored |

## Forecast features (`serving.forecast_features`)
Dense grid: every zone seen in the batch x every hour of the window (zero-demand hours
present; `target_avg_fare` NULL then). Targets: `target_trip_count`, `target_avg_fare`.
Calendar features (hour, ISO day-of-week, month, is_weekend). History features use only
strictly earlier hours (`lag_1h/24h/168h`, rolling mean of the 24 preceding hours) - verified
by a unit test that perturbs a target and asserts its own features do not change.
Cold start: first 24/168 hours of a batch have NULL lags. Zones with no trips in the whole
batch are absent from the grid.
