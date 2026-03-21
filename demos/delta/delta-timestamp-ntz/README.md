# Delta Timestamps — Timezone-Free & Precision Handling

Demonstrates timezone-free timestamp handling in Delta tables using VARCHAR
columns for an airline scheduling system.

## Data Story

An airline scheduling system stores flight times in multiple formats: local
departure/arrival times (timezone-free, as the traveler sees them on the
departure board) and UTC times for system coordination. This is a common
pattern when timezone context is implicit in the origin/destination airport
rather than embedded in the timestamp itself.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `flight_schedule` | Delta Table | 45 | Airline flights with local + UTC timestamps |

## Schema

**flight_schedule:** `id INT, flight_code VARCHAR, origin VARCHAR, destination VARCHAR, departure_local VARCHAR, arrival_local VARCHAR, departure_utc VARCHAR, duration_minutes INT, status VARCHAR, gate VARCHAR`

## Flight Distribution

- **Domestic US (ids 1-25):** 25 flights between JFK, LAX, ORD, ATL, DFW
- **International (ids 26-35):** 10 flights to/from LHR, NRT, CDG, SYD
- **Red-eye/Overnight (ids 36-45):** 10 flights where arrival crosses midnight

## Operations

1. INSERT 25 rows — domestic US flights with local and UTC times
2. INSERT 10 rows — international flights with varying UTC offsets
3. INSERT 10 rows — red-eye/overnight flights (next-day arrivals)
4. UPDATE — delay 5 flights (add 90 minutes, set status='delayed')
5. UPDATE — cancel 3 flights (set status='cancelled')

## Verification

8 automated PASS/FAIL checks verify total row count (45), domestic count (25),
international count (20), delayed count (5), cancelled count (3), on-time
count (37), JFK departures (9), and longest flight duration (1140 minutes).
