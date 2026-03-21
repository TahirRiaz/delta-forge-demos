-- ============================================================================
-- Delta Timestamps NTZ (No TimeZone) -- Educational Queries
-- ============================================================================
-- WHAT: Timezone-free timestamp storage preserves literal date/time values
--       without embedding timezone metadata in the Delta table.
-- WHY:  Global systems need "wall clock" times (e.g., flight boards) that are
--       meaningful in their local context, separate from UTC coordination.
-- HOW:  Delta stores timestamps as VARCHAR or TIMESTAMP_NTZ columns in Parquet
--       files, so the value "08:30" stays "08:30" regardless of the reader's
--       timezone setting -- no implicit conversion occurs.
-- ============================================================================


-- ============================================================================
-- EXPLORE: What does the flight schedule look like?
-- ============================================================================
-- Each flight stores BOTH a local time (what the traveler sees on the departure
-- board) and a UTC time (for system-level coordination across timezones).
-- Notice that departure_local and departure_utc differ by the origin's offset.

ASSERT VALUE status = 'delayed' WHERE id = 6
ASSERT ROW_COUNT = 10
SELECT id, flight_code, origin, destination,
       departure_local, departure_utc,
       duration_minutes, status
FROM {{zone_name}}.delta_demos.flight_schedule
ORDER BY id
LIMIT 10;


-- ============================================================================
-- EXPLORE: How do local vs UTC times differ across origins?
-- ============================================================================
-- The gap between departure_local and departure_utc reveals the timezone offset
-- of the origin airport. JFK is UTC-4 (EDT), LAX is UTC-7 (PDT), etc.
-- With NTZ storage, these offsets are not stored -- they are implicit in the
-- origin/destination context, keeping the data timezone-agnostic.

ASSERT ROW_COUNT = 9
SELECT origin,
       COUNT(*) AS flights,
       MIN(departure_local) AS earliest_local,
       MIN(departure_utc) AS earliest_utc
FROM {{zone_name}}.delta_demos.flight_schedule
WHERE status = 'on_time'
GROUP BY origin
ORDER BY origin;


-- ============================================================================
-- LEARN: Overnight and red-eye flights -- date boundary crossings
-- ============================================================================
-- Timezone-free storage is especially important for overnight flights where
-- the arrival date is the NEXT day. Without NTZ, timezone conversion could
-- shift the date incorrectly. Here we find flights where the arrival local
-- date differs from the departure local date -- a classic red-eye pattern.

ASSERT ROW_COUNT = 14
SELECT id, flight_code, origin, destination,
       departure_local, arrival_local,
       duration_minutes
FROM {{zone_name}}.delta_demos.flight_schedule
WHERE arrival_local > departure_local
  AND SUBSTRING(arrival_local, 1, 10) != SUBSTRING(departure_local, 1, 10)
ORDER BY duration_minutes DESC;


-- ============================================================================
-- LEARN: International flights and UTC coordination
-- ============================================================================
-- For international routes, the local times at origin and destination are in
-- completely different timezones. The UTC column provides a single reference
-- frame for scheduling, conflict detection, and sequencing -- while the local
-- times remain human-readable for passengers and ground staff.

ASSERT VALUE duration_minutes = 1140 WHERE id = 42
ASSERT ROW_COUNT = 10
SELECT id, flight_code, origin, destination,
       departure_local AS depart_local,
       arrival_local AS arrive_local,
       departure_utc AS depart_utc,
       duration_minutes
FROM {{zone_name}}.delta_demos.flight_schedule
WHERE origin NOT IN ('JFK','LAX','ORD','ATL','DFW')
   OR destination NOT IN ('JFK','LAX','ORD','ATL','DFW')
ORDER BY duration_minutes DESC
LIMIT 10;


-- ============================================================================
-- LEARN: Status transitions -- how DML preserves timestamp integrity
-- ============================================================================
-- When flights are delayed or cancelled via UPDATE, the timestamp values must
-- remain stable. Delta's copy-on-write ensures the original Parquet files are
-- preserved (until VACUUM), while new files contain the updated rows.
-- The NTZ timestamps are never reinterpreted during these rewrites.

-- Verify flight counts per status: cancelled=3, delayed=5, on_time=37
ASSERT VALUE flight_count = 3 WHERE status = 'cancelled'
ASSERT VALUE flight_count = 5 WHERE status = 'delayed'
ASSERT VALUE flight_count = 37 WHERE status = 'on_time'
ASSERT ROW_COUNT = 3
SELECT status, COUNT(*) AS flight_count,
       MIN(departure_local) AS earliest_departure,
       MAX(departure_local) AS latest_departure
FROM {{zone_name}}.delta_demos.flight_schedule
GROUP BY status
ORDER BY status;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 45
ASSERT ROW_COUNT = 45
SELECT * FROM {{zone_name}}.delta_demos.flight_schedule;

-- Verify 25 domestic flights (both origin and destination are US airports)
ASSERT VALUE domestic_count = 25
SELECT COUNT(*) AS domestic_count FROM {{zone_name}}.delta_demos.flight_schedule
WHERE origin IN ('JFK','LAX','ORD','ATL','DFW') AND destination IN ('JFK','LAX','ORD','ATL','DFW');

-- Verify 20 international flights
ASSERT VALUE international_count = 20
SELECT COUNT(*) AS international_count FROM {{zone_name}}.delta_demos.flight_schedule
WHERE NOT (origin IN ('JFK','LAX','ORD','ATL','DFW') AND destination IN ('JFK','LAX','ORD','ATL','DFW'));

-- Verify 5 delayed flights
ASSERT VALUE delayed_count = 5
SELECT COUNT(*) AS delayed_count FROM {{zone_name}}.delta_demos.flight_schedule WHERE status = 'delayed';

-- Verify 3 cancelled flights
ASSERT VALUE cancelled_count = 3
SELECT COUNT(*) AS cancelled_count FROM {{zone_name}}.delta_demos.flight_schedule WHERE status = 'cancelled';

-- Verify 37 on-time flights
ASSERT VALUE on_time_count = 37
SELECT COUNT(*) AS on_time_count FROM {{zone_name}}.delta_demos.flight_schedule WHERE status = 'on_time';

-- Verify 9 JFK departures
ASSERT VALUE jfk_departures = 9
SELECT COUNT(*) AS jfk_departures FROM {{zone_name}}.delta_demos.flight_schedule WHERE origin = 'JFK';

-- Verify longest flight is 1140 minutes
ASSERT VALUE longest_flight = 1140
SELECT MAX(duration_minutes) AS longest_flight FROM {{zone_name}}.delta_demos.flight_schedule;
