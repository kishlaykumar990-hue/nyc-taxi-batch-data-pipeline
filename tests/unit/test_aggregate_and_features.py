"""Aggregation correctness and forecasting-feature construction (incl. leakage)."""
import pytest
from pyspark.sql import functions as F

from src.common.config import QualityLimits
from src.spark.transform import aggregate_hourly, build_features, flag_rows, split_flagged

W_START, W_END = "2024-01-01 00:00:00", "2024-01-03 00:00:00"   # 48 hourly buckets


def _accepted(df):
    accepted, _ = split_flagged(flag_rows(df, W_START, W_END, QualityLimits()))
    return accepted


def _trip(hour, minute=0, zone=161, fare=10.0, dist=2.0, mins=10, day=1):
    start = f"2024-01-{day:02d} {hour:02d}:{minute:02d}:00"
    end_total = hour * 60 + minute + mins
    end = f"2024-01-{day:02d} {end_total // 60:02d}:{end_total % 60:02d}:00"
    return dict(pickup_ts=start, dropoff_ts=end, pu_location_id=zone, fare_amount=fare,
                total_amount=fare + 3, trip_distance=dist)


def test_hourly_aggregation_values(make_trips):
    acc = _accepted(make_trips(
        _trip(10, 5, fare=10.0, dist=1.0, mins=10),
        _trip(10, 20, fare=20.0, dist=3.0, mins=20),
        _trip(10, 50, fare=30.0, dist=5.0, mins=30),
        _trip(11, 0, fare=99.0),                     # next hour, same zone
        _trip(10, 10, zone=237, fare=7.0),           # same hour, other zone
    ))
    rows = {(r["pickup_hour"].hour, r["pu_location_id"]): r for r in aggregate_hourly(acc).collect()}
    r = rows[(10, 161)]
    assert r["trip_count"] == 3
    assert r["avg_fare"] == pytest.approx(20.0)
    assert r["p50_fare"] == pytest.approx(20.0)
    assert r["sum_fare_amount"] == pytest.approx(60.0)
    assert r["sum_total_amount"] == pytest.approx(69.0)          # fare + 3 per trip
    assert r["avg_trip_distance"] == pytest.approx(3.0)
    assert r["avg_duration_min"] == pytest.approx(20.0)
    assert rows[(11, 161)]["trip_count"] == 1
    assert rows[(10, 237)]["trip_count"] == 1
    assert len(rows) == 3


def test_aggregate_trip_total_reconciles_with_accepted_rows(make_trips):
    acc = _accepted(make_trips(*[_trip(h, m) for h in range(0, 6) for m in (0, 15, 45)]))
    total = aggregate_hourly(acc).agg(F.sum("trip_count")).collect()[0][0]
    assert total == acc.count() == 18


def test_rejected_rows_never_reach_aggregates(make_trips):
    df = make_trips(_trip(9), dict(_trip(9), fare_amount=-50.0))
    agg = aggregate_hourly(_accepted(df)).collect()
    assert len(agg) == 1 and agg[0]["trip_count"] == 1


def _features(spark, make_trips, trips):
    acc = _accepted(make_trips(*trips))
    hourly = aggregate_hourly(acc)
    return build_features(spark, hourly, W_START, W_END)


def test_feature_grid_is_dense_with_zero_demand_hours(spark, make_trips):
    feats = _features(spark, make_trips, [_trip(5), _trip(9, zone=237)])
    # 2 observed zones x 48 hours, including hours with no trips
    assert feats.count() == 2 * 48
    zero_hours = feats.filter((F.col("pu_location_id") == 161) & (F.col("target_trip_count") == 0)).count()
    assert zero_hours == 47
    assert feats.filter(F.col("target_trip_count") == 0).filter(F.col("target_avg_fare").isNotNull()).count() == 0


def test_calendar_features(spark, make_trips):
    feats = _features(spark, make_trips, [_trip(5)])
    by_hour = {r["pickup_hour"].strftime("%Y-%m-%d %H"): r for r in feats.collect()}
    monday = by_hour["2024-01-01 05"]                 # 2024-01-01 is a Monday
    assert (monday["hour_of_day"], monday["day_of_week"], monday["month"], monday["is_weekend"]) == (5, 1, 1, False)
    sunday_acc = _accepted_for_weekend(spark, make_trips)
    assert sunday_acc


def _accepted_for_weekend(spark, make_trips):
    # window covering Sat 2024-01-06 .. Sun 2024-01-07
    start, end = "2024-01-06 00:00:00", "2024-01-08 00:00:00"
    df = make_trips(dict(pickup_ts="2024-01-06 12:00:00", dropoff_ts="2024-01-06 12:10:00"))
    acc, _ = split_flagged(flag_rows(df, start, end, QualityLimits()))
    feats = build_features(spark, aggregate_hourly(acc), start, end)
    rows = {r["pickup_hour"].strftime("%Y-%m-%d"): r for r in feats.collect()}
    assert rows["2024-01-06"]["day_of_week"] == 6 and rows["2024-01-06"]["is_weekend"] is True
    assert rows["2024-01-07"]["day_of_week"] == 7 and rows["2024-01-07"]["is_weekend"] is True
    return True


def test_lag_features_use_only_earlier_hours(spark, make_trips):
    # 3 trips at 05:xx, 2 at 06:xx on day 1; 4 at 05:xx on day 2 (24 h later)
    trips = [_trip(5, m) for m in (0, 10, 20)] + [_trip(6, m) for m in (0, 10)] + \
            [_trip(5, m, day=2) for m in (0, 10, 20, 30)]
    feats = _features(spark, make_trips, trips)
    by_hour = {r["pickup_hour"].strftime("%d %H"): r for r in feats.collect()}
    assert by_hour["01 06"]["lag_1h_trip_count"] == 3                       # previous hour
    assert by_hour["01 07"]["lag_1h_trip_count"] == 2
    assert by_hour["02 05"]["target_trip_count"] == 4
    assert by_hour["02 05"]["lag_24h_trip_count"] == 3                       # same hour yesterday
    assert by_hour["02 05"]["lag_1h_trip_count"] == 0
    # cold start: no history for the first hours of the window
    assert by_hour["01 00"]["lag_1h_trip_count"] is None
    assert by_hour["01 12"]["lag_24h_trip_count"] is None
    assert by_hour["02 05"]["lag_168h_trip_count"] is None                   # window shorter than a week


def test_rolling_mean_excludes_current_hour(spark, make_trips):
    trips = [_trip(5, m) for m in (0, 10)] + [_trip(6, 0)]                    # 2 trips @05, 1 @06
    feats = _features(spark, make_trips, trips)
    by_hour = {r["pickup_hour"].strftime("%d %H"): r for r in feats.collect()}
    # mean of the 6 preceding hours that exist (00..05) = (0+0+0+0+0+2)/6, current hour NOT included
    assert by_hour["01 06"]["rolling_mean_24h_trip_count"] == pytest.approx(2 / 6)
    assert by_hour["01 00"]["rolling_mean_24h_trip_count"] is None            # empty preceding frame


def test_no_target_leakage_changing_a_target_never_changes_its_own_features(spark, make_trips):
    base = [_trip(5, m) for m in (0, 10)] + [_trip(6, 0)] + [_trip(7, 0)]
    more = base + [_trip(6, m) for m in (10, 20, 30, 40)]                     # inflate the 06:00 hour only
    f_base = {r["pickup_hour"].strftime("%d %H"): r for r in _features(spark, make_trips, base).collect()}
    f_more = {r["pickup_hour"].strftime("%d %H"): r for r in _features(spark, make_trips, more).collect()}
    hist = ["lag_1h_trip_count", "lag_24h_trip_count", "lag_168h_trip_count",
            "rolling_mean_24h_trip_count", "lag_1h_avg_fare"]
    # features of 06:00 itself and everything before are unchanged ...
    for key in ("01 00", "01 05", "01 06"):
        for col in hist:
            assert f_base[key][col] == f_more[key][col], f"{col} at {key} leaked the target"
    # ... while the *next* hour legitimately sees the larger history
    assert f_base["01 07"]["lag_1h_trip_count"] == 1
    assert f_more["01 07"]["lag_1h_trip_count"] == 5
