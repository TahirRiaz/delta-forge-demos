#!/usr/bin/env python3
"""
Iceberg V2 Multi-Partition Weather Readings — Data Verification
================================================================
Reads the weather_readings table through the Iceberg metadata chain and
verifies 450 observations across 5 regions and 3 years (15 partitions).

Usage:
    python verify.py <data_root_path> [--verbose]

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (
    read_iceberg_table, ok, fail, info,
    assert_row_count, assert_sum, assert_avg, assert_min, assert_max,
    assert_distinct_count, assert_count_where, assert_format_version,
    reset_counters,
)
from verify_lib import print_header, print_summary, exit_with_status


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-multi-partition demo"
    )
    parser.add_argument("data_root", help="Parent folder containing weather_readings/")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc

    data_root = os.path.abspath(args.data_root)
    table_path = os.path.join(data_root, "weather_readings")

    print_header("Multi-Partition (Weather) — Data Verification")
    print(f"  Data root: {data_root}")

    if not os.path.isdir(os.path.join(table_path, "metadata")):
        print(f"\nError: {table_path}/metadata not found")
        sys.exit(1)

    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg metadata chain")

    if args.verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 450)

    # Region breakdown (90 each)
    assert_count_where(table, "region", "Africa", 90)
    assert_count_where(table, "region", "Asia", 90)
    assert_count_where(table, "region", "Europe", 90)
    assert_count_where(table, "region", "North America", 90)
    assert_count_where(table, "region", "South America", 90)

    # Distinct counts
    assert_distinct_count(table, "region", 5)
    assert_distinct_count(table, "station_id", 15)
    assert_distinct_count(table, "condition", 5)

    # Condition distribution
    assert_count_where(table, "condition", "Clear", 83)
    assert_count_where(table, "condition", "Cloudy", 102)
    assert_count_where(table, "condition", "Rain", 88)
    assert_count_where(table, "condition", "Snow", 94)
    assert_count_where(table, "condition", "Storm", 83)

    # Temperature ranges
    assert_min(table, "temperature_c", -9.9)
    assert_max(table, "temperature_c", 39.5)

    # Humidity ranges
    assert_min(table, "humidity_pct", 20.9)
    assert_max(table, "humidity_pct", 99.9)

    # Total wind
    assert_sum(table, "wind_speed_kmh", 25320.80, label="total wind")

    # Avg precipitation (non-null)
    assert_avg(table, "precipitation_mm", 25.01, label="avg precipitation")

    # Null precipitation count
    null_precip = pc.sum(pc.is_null(table.column("precipitation_mm"))).as_py()
    if null_precip == 58:
        ok(f"NULL precipitation_mm count = 58")
    else:
        fail(f"NULL precipitation_mm count = {null_precip}, expected 58")

    # Extreme readings (temp > 35 or temp < -5)
    extreme = table.filter(pc.or_(
        pc.greater(table.column("temperature_c"), 35),
        pc.less(table.column("temperature_c"), -5)
    ))
    assert_row_count(extreme, 41, label="extreme temp readings")

    # Per-station avg temp (select key ones)
    for station, exp_avg in [("WX-AF001", 23.84), ("WX-AS001", 12.98),
                              ("WX-EU001", 12.99), ("WX-NA001", 12.46), ("WX-SA001", 24.61)]:
        mask = pc.equal(table.column("station_id"), station)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("temperature_c")).as_py(), 2)
        if actual == exp_avg:
            ok(f"AVG(temperature_c) WHERE station_id={station!r} = {exp_avg}")
        else:
            fail(f"AVG(temperature_c) WHERE station_id={station!r} = {actual}, expected {exp_avg}")

    # Specific extreme rows
    row_439 = table.filter(pc.equal(table.column("reading_id"), 439))
    if row_439.num_rows > 0:
        temp = round(row_439.column("temperature_c")[0].as_py(), 1)
        if temp == 39.5:
            ok("reading_id=439 temperature_c = 39.5")
        else:
            fail(f"reading_id=439 temperature_c = {temp}, expected 39.5")
    row_53 = table.filter(pc.equal(table.column("reading_id"), 53))
    if row_53.num_rows > 0:
        temp = round(row_53.column("temperature_c")[0].as_py(), 1)
        if temp == -9.9:
            ok("reading_id=53 temperature_c = -9.9")
        else:
            fail(f"reading_id=53 temperature_c = {temp}, expected -9.9")

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
