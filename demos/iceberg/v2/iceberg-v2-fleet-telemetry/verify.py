#!/usr/bin/env python3
"""
Iceberg V2 Fleet Telemetry — Data Verification
================================================
Reads the fleet_telemetry table through the Iceberg metadata chain and
verifies 450 GPS pings across 3 regional fleets match expected values.

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
    assert_row_count, assert_distinct_count,
    assert_count_where, assert_format_version,
    reset_counters,
)
from verify_lib import print_header, print_summary, exit_with_status


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-fleet-telemetry demo"
    )
    parser.add_argument("data_root", help="Parent folder containing fleet_telemetry/")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc

    data_root = os.path.abspath(args.data_root)
    table_path = os.path.join(data_root, "fleet_telemetry")

    print_header("Fleet Telemetry — Data Verification")
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

    # Per-fleet counts (150 each)
    assert_count_where(table, "fleet", "East-Coast", 150)
    assert_count_where(table, "fleet", "Midwest", 150)
    assert_count_where(table, "fleet", "West-Coast", 150)

    # Vehicle type distribution
    assert_count_where(table, "vehicle_type", "Box-Truck", 132)
    assert_count_where(table, "vehicle_type", "Delivery-Van", 156)
    assert_count_where(table, "vehicle_type", "Semi-Truck", 162)

    # Average speed by fleet
    for fleet, exp_avg in [("East-Coast", 37.27), ("Midwest", 37.89), ("West-Coast", 37.13)]:
        mask = pc.equal(table.column("fleet"), fleet)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("speed_mph")).as_py(), 2)
        if actual == exp_avg:
            ok(f"AVG(speed_mph) WHERE fleet={fleet!r} = {exp_avg}")
        else:
            fail(f"AVG(speed_mph) WHERE fleet={fleet!r} = {actual}, expected {exp_avg}")

    # Total idle minutes by fleet
    for fleet, exp_idle in [("East-Coast", 3355), ("Midwest", 3439), ("West-Coast", 3751)]:
        mask = pc.equal(table.column("fleet"), fleet)
        filtered = table.filter(mask)
        actual = pc.sum(filtered.column("idle_minutes")).as_py()
        if actual == exp_idle:
            ok(f"SUM(idle_minutes) WHERE fleet={fleet!r} = {exp_idle}")
        else:
            fail(f"SUM(idle_minutes) WHERE fleet={fleet!r} = {actual}, expected {exp_idle}")

    # Harsh braking events by fleet
    for fleet, exp_harsh in [("East-Coast", 27), ("Midwest", 20), ("West-Coast", 31)]:
        mask = pc.equal(table.column("fleet"), fleet)
        filtered = table.filter(mask)
        harsh_count = pc.sum(pc.equal(filtered.column("harsh_braking"), True)).as_py()
        if harsh_count == exp_harsh:
            ok(f"Harsh braking WHERE fleet={fleet!r} = {exp_harsh}")
        else:
            fail(f"Harsh braking WHERE fleet={fleet!r} = {harsh_count}, expected {exp_harsh}")

    # Total harsh braking
    total_harsh = pc.sum(pc.equal(table.column("harsh_braking"), True)).as_py()
    if total_harsh == 78:
        ok(f"Total harsh braking = 78")
    else:
        fail(f"Total harsh braking = {total_harsh}, expected 78")

    # Speeding (> 65 mph)
    speeding = table.filter(pc.greater(table.column("speed_mph"), 65))
    assert_row_count(speeding, 53, label="speed_mph > 65")

    # Low fuel (< 20%)
    low_fuel = table.filter(pc.less(table.column("fuel_level_pct"), 20))
    assert_row_count(low_fuel, 74, label="fuel_level_pct < 20")

    # High engine temp (> 220F)
    high_temp = table.filter(pc.greater(table.column("engine_temp_f"), 220))
    assert_row_count(high_temp, 157, label="engine_temp_f > 220")

    # Distinct counts
    assert_distinct_count(table, "vehicle_id", 450)
    assert_distinct_count(table, "driver_id", 98)
    assert_distinct_count(table, "route_id", 15)
    assert_distinct_count(table, "fleet", 3)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
