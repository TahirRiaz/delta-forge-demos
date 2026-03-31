#!/usr/bin/env python3
"""
Iceberg Energy Grid Monitoring — Data Verification
====================================================
Reads the grid_readings table purely through the Iceberg metadata chain
and verifies 600 smart meter readings across 3 regions match expected values.

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
        description="Verify Iceberg data for energy-grid-monitoring demo"
    )
    parser.add_argument("data_root", help="Path to the Iceberg table root (the grid_readings directory itself)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc

    # The data_root IS the table root (contains metadata/ and data/)
    data_root = os.path.abspath(args.data_root)

    print_header("Energy Grid Monitoring — Iceberg Data Verification")
    print(f"  Data root: {data_root}")

    if not os.path.isdir(os.path.join(data_root, "metadata")):
        print(f"\nError: {data_root}/metadata not found")
        sys.exit(1)

    table, metadata = read_iceberg_table(data_root)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg metadata chain")

    if args.verbose:
        info(f"Columns: {table.column_names}")

    # Format version
    assert_format_version(metadata, 2)

    # Row count
    assert_row_count(table, 600)

    # Per-region counts (200 each)
    assert_count_where(table, "region", "North", 200)
    assert_count_where(table, "region", "South", 200)
    assert_count_where(table, "region", "East", 200)

    # Total energy (4 decimal precision needed)
    total_energy = round(pc.sum(table.column("energy_kwh")).as_py(), 4)
    if total_energy == 993.2375:
        ok(f"SUM(energy_kwh) = 993.2375 (total energy)")
    else:
        fail(f"SUM(energy_kwh) = {total_energy}, expected 993.2375 (total energy)")

    # Per-region energy totals
    for region, expected in [("North", 341.6525), ("South", 324.7675), ("East", 326.8175)]:
        mask = pc.equal(table.column("region"), region)
        filtered = table.filter(mask)
        actual = round(pc.sum(filtered.column("energy_kwh")).as_py(), 4)
        if actual == expected:
            ok(f"SUM(energy_kwh) WHERE region={region!r} = {expected}")
        else:
            fail(f"SUM(energy_kwh) WHERE region={region!r} = {actual}, expected {expected}")

    # Meter type distribution
    assert_count_where(table, "meter_type", "Commercial", 208)
    assert_count_where(table, "meter_type", "Industrial", 198)
    assert_count_where(table, "meter_type", "Residential", 194)

    # Voltage distribution
    assert_count_where(table, "voltage", 220, 187)
    assert_count_where(table, "voltage", 230, 205)
    assert_count_where(table, "voltage", 240, 208)

    # High-power readings (power_kw > 10)
    high_power = table.filter(pc.greater(table.column("power_kw"), 10))
    assert_row_count(high_power, 102, label="power_kw > 10")

    # Low power factor (< 85)
    low_pf = table.filter(pc.less(table.column("power_factor"), 85))
    assert_row_count(low_pf, 132, label="power_factor < 85")

    # Distinct counts
    assert_distinct_count(table, "meter_id", 600)
    assert_distinct_count(table, "substation", 15)

    # Avg power by meter type
    for mtype, expected_avg in [("Commercial", 6.44), ("Industrial", 6.75), ("Residential", 6.68)]:
        mask = pc.equal(table.column("meter_type"), mtype)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("power_kw")).as_py(), 2)
        if actual == expected_avg:
            ok(f"AVG(power_kw) WHERE meter_type={mtype!r} = {expected_avg}")
        else:
            fail(f"AVG(power_kw) WHERE meter_type={mtype!r} = {actual}, expected {expected_avg}")

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
