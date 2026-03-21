-- ============================================================================
-- Delta Variant Type -- Semi-Structured Data -- Educational Queries
-- ============================================================================
-- WHAT: The VARIANT pattern stores semi-structured data (JSON payloads) with
--       different shapes in a single column, avoiding rigid schema definitions.
-- WHY:  Event-driven systems produce messages with varying structures. Forcing
--       a fixed schema creates nulls, schema explosions, or data loss.
-- HOW:  Delta stores VARCHAR columns containing JSON strings in Parquet. Each
--       row can have a completely different JSON structure. Queries use string
--       functions (LIKE, SUBSTRING) to extract values at read time.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Different event types with different payload structures
-- ============================================================================
-- Each event_type has a fundamentally different payload schema:
--   login:    {"user", "ip", "method"}
--   purchase: {"item", "amount", "currency"}
--   error:    {"code", "message", "endpoint"}
--   system:   {"action", "component", "duration_ms"}
-- All stored in the SAME 'payload' VARCHAR column. This is the variant pattern.

ASSERT ROW_COUNT = 4
SELECT id, event_type, source, payload
FROM {{zone_name}}.delta_demos.api_events
WHERE id IN (1, 21, 36, 46)
ORDER BY id;


-- ============================================================================
-- EXPLORE: Metadata column -- another semi-structured field
-- ============================================================================
-- The metadata column also varies by event type:
--   login from web-app:     {"browser", "os"}
--   login from mobile-app:  {"device", "os"}
--   login from api-gateway: {"client", "version"}
-- This shows how even within one event_type, the metadata shape differs
-- based on the source. A rigid schema would need dozens of nullable columns.

ASSERT ROW_COUNT = 8
SELECT id, event_type, source, metadata
FROM {{zone_name}}.delta_demos.api_events
WHERE event_type = 'login'
ORDER BY id
LIMIT 8;


-- ============================================================================
-- LEARN: Querying semi-structured data with LIKE patterns
-- ============================================================================
-- Without native JSON functions, you can use LIKE to filter on payload content.
-- This is how HTTP 500 errors are identified and marked as critical in the next section.
-- While less precise than JSON path extraction, LIKE works on any SQL engine.

-- Verify all 10 error events are present
ASSERT ROW_COUNT = 10
SELECT id, event_type, payload, severity
FROM {{zone_name}}.delta_demos.api_events
WHERE event_type = 'error'
ORDER BY id;


-- ============================================================================
-- LEARN: Severity classification via UPDATE on semi-structured data
-- ============================================================================
-- HTTP 500 errors can be upgraded to 'critical' severity using LIKE pattern
-- matching against the JSON payload. Delta's copy-on-write creates new Parquet
-- files with the updated severity without rewriting the entire dataset.

-- Upgrade HTTP 500 error events to 'critical' severity
UPDATE {{zone_name}}.delta_demos.api_events
SET severity = 'critical'
WHERE event_type = 'error' AND payload LIKE '%"code":500%';

-- Verify 6 critical events (HTTP 500 errors upgraded to critical)
ASSERT VALUE critical_count = 6
SELECT COUNT(*) AS critical_count FROM {{zone_name}}.delta_demos.api_events WHERE severity = 'critical';

-- Verify severity distribution: info=39, warning=5, critical=6 (no 'error' severity remains)
ASSERT ROW_COUNT = 3
ASSERT VALUE event_count = 39 WHERE severity = 'info'
ASSERT VALUE event_count = 5 WHERE severity = 'warning'
ASSERT VALUE event_count = 6 WHERE severity = 'critical'
SELECT severity, COUNT(*) AS event_count,
       COUNT(DISTINCT event_type) AS event_types
FROM {{zone_name}}.delta_demos.api_events
GROUP BY severity
ORDER BY severity;


-- ============================================================================
-- LEARN: Source distribution -- the multi-channel pattern
-- ============================================================================
-- The variant pattern shines when data comes from multiple sources, each with
-- its own payload format. Here we see 4 sources (web-app, mobile-app,
-- api-gateway, internal) feeding into one table. In a rigid schema, you would
-- need separate tables or many nullable columns for each source's fields.

-- Verify 4 distinct sources
ASSERT VALUE distinct_sources = 4
SELECT COUNT(DISTINCT source) AS distinct_sources FROM {{zone_name}}.delta_demos.api_events;

ASSERT ROW_COUNT = 13
SELECT source, event_type, COUNT(*) AS events
FROM {{zone_name}}.delta_demos.api_events
GROUP BY source, event_type
ORDER BY source, event_type;


-- ============================================================================
-- LEARN: Purchase events -- extracting meaning from JSON strings
-- ============================================================================
-- Purchase payloads contain amount and currency fields as JSON. While a native
-- VARIANT type would allow direct field access (payload.amount), the VARCHAR
-- approach requires parsing. This tradeoff is: flexible writes, harder reads.
-- The Delta VARIANT type (protocol v3) improves this with native JSON support.

ASSERT ROW_COUNT = 8
SELECT id, source, payload, metadata
FROM {{zone_name}}.delta_demos.api_events
WHERE event_type = 'purchase'
ORDER BY id
LIMIT 8;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 50
ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.delta_demos.api_events;

-- Verify 20 login events
ASSERT VALUE login_count = 20
SELECT COUNT(*) AS login_count FROM {{zone_name}}.delta_demos.api_events WHERE event_type = 'login';

-- Verify 15 purchase events
ASSERT VALUE purchase_count = 15
SELECT COUNT(*) AS purchase_count FROM {{zone_name}}.delta_demos.api_events WHERE event_type = 'purchase';

-- Verify 10 error events
ASSERT VALUE error_count = 10
SELECT COUNT(*) AS error_count FROM {{zone_name}}.delta_demos.api_events WHERE event_type = 'error';

-- Verify 5 system events
ASSERT VALUE system_count = 5
SELECT COUNT(*) AS system_count FROM {{zone_name}}.delta_demos.api_events WHERE event_type = 'system';

-- Verify all payloads are non-null — every event type stores a non-null JSON string
ASSERT VALUE payload_not_null = 50
SELECT COUNT(*) AS payload_not_null FROM {{zone_name}}.delta_demos.api_events WHERE payload IS NOT NULL;
