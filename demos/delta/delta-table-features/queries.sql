-- ============================================================================
-- Delta Protocol & Table Features — Educational Queries
-- ============================================================================
-- WHAT: Table features are protocol-level capabilities declared via
--       TBLPROPERTIES that control Delta table behavior.
-- WHY:  Features like Change Data Feed, deletion vectors, and append-only
--       mode are not enabled by default. Each feature changes how data is
--       written and what metadata is tracked, so they must be explicitly
--       opted into. This also ensures older readers can detect when they
--       lack support for a required feature.
-- HOW:  Features are stored in the Protocol action of the transaction log.
--       Reader features are "readerFeatures" and writer features are
--       "writerFeatures." An engine that does not support a required feature
--       will refuse to read/write the table rather than silently corrupt it.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — Two Tables with Different Feature Sets
-- ============================================================================
-- The setup created two tables with different TBLPROPERTIES:
--
-- 1. feature_demo: enableChangeDataFeed = true
--    Supports INSERT, UPDATE, DELETE. CDC records are tracked.
--
-- 2. audit_trail: appendOnly = true, enableChangeDataFeed = true
--    Only INSERT is allowed. UPDATE and DELETE are blocked by the protocol.
--    This guarantees the audit trail is immutable — perfect for compliance.

ASSERT VALUE row_count = 30
ASSERT VALUE categories = 3
ASSERT VALUE statuses = 2
ASSERT ROW_COUNT = 1
SELECT 'feature_demo' AS table_name,
       COUNT(*) AS row_count,
       COUNT(DISTINCT category) AS categories,
       COUNT(DISTINCT status) AS statuses
FROM {{zone_name}}.delta_demos.feature_demo;


-- ============================================================================
-- LEARN: Change Data Feed (CDC)
-- ============================================================================
-- When enableChangeDataFeed is true, Delta records the before and after
-- images of modified rows in a special _change_data directory alongside
-- the regular data files. This enables downstream consumers to process
-- only what changed rather than re-reading the entire table.
--
-- Let's see the current trial items before we update them:

ASSERT ROW_COUNT = 5
SELECT id, name, value, status
FROM {{zone_name}}.delta_demos.feature_demo
WHERE status = 'trial'
ORDER BY id;


-- ============================================================================
-- ACTION: UPDATE — Convert trial items to active (triggers CDC)
-- ============================================================================
-- This UPDATE changes 5 rows: status trial -> active, with a 20% discount.
-- Because enableChangeDataFeed is true, Delta writes pre-image (old row) and
-- post-image (new row) records to the change data files. Downstream consumers
-- can read these CDC records to process only the changed rows.

ASSERT ROW_COUNT = 5
UPDATE {{zone_name}}.delta_demos.feature_demo
SET status = 'active',
    value = ROUND(value * 0.80, 2)
WHERE status = 'trial';


-- ============================================================================
-- LEARN: How UPDATEs Work with CDC Enabled
-- ============================================================================
-- The 5 trial items (ids 7, 10, 15, 22, 30) were converted to active with
-- a 20% discount. With CDC enabled, Delta wrote pre-image and post-image
-- records. Let's see the updated items and their new discounted values:

ASSERT ROW_COUNT = 5
SELECT id, name, category, value, status
FROM {{zone_name}}.delta_demos.feature_demo
WHERE id IN (7, 10, 15, 22, 30)
ORDER BY id;


-- Confirm no trial items remain after the update — only 'active' status:
ASSERT VALUE status = 'active'
ASSERT VALUE items = 30
ASSERT ROW_COUNT = 1
SELECT status,
       COUNT(*) AS items,
       ROUND(MIN(value), 2) AS min_value,
       ROUND(MAX(value), 2) AS max_value,
       ROUND(AVG(value), 2) AS avg_value
FROM {{zone_name}}.delta_demos.feature_demo
GROUP BY status;


-- ============================================================================
-- LEARN: How DELETEs Work — Deletion Vectors vs. Copy-on-Write
-- ============================================================================
-- Delta supports two strategies for DELETE:
--
-- 1. Copy-on-Write (default): Rewrites entire Parquet files, omitting
--    deleted rows. Simple but expensive for large files.
--
-- 2. Deletion Vectors: Writes a compact bitmap marking which rows are
--    deleted, without rewriting the data file. Much faster for selective
--    deletes. The deleted rows are physically removed later during OPTIMIZE.
--
-- Let's see which items have value < 46.0 before we delete them:

ASSERT ROW_COUNT = 3
SELECT id, name, category, value
FROM {{zone_name}}.delta_demos.feature_demo
WHERE value < 46.0
ORDER BY value;


-- ============================================================================
-- ACTION: DELETE — Remove lowest-value items (triggers deletion vectors)
-- ============================================================================
-- This DELETE removes 3 rows with value < 46.0:
--   id=9  Iota Adapter    (35.00)
--   id=6  Zeta Component  (45.00)
--   id=30 Zeta-2 Component (was 55.00 * 0.80 = 44.00 after discount)
--
-- With deletion vectors, Delta writes a compact bitmap marking these rows
-- as deleted rather than rewriting the entire data file.

ASSERT ROW_COUNT = 3
DELETE FROM {{zone_name}}.delta_demos.feature_demo
WHERE value < 46.0;


-- Confirm the deleted items are gone — hardware items remaining:
ASSERT ROW_COUNT = 10
SELECT id, name, value
FROM {{zone_name}}.delta_demos.feature_demo
WHERE category = 'hardware'
ORDER BY value;


-- ============================================================================
-- LEARN: Append-Only Mode — The Audit Trail
-- ============================================================================
-- The audit_trail table has appendOnly = true. This table feature tells
-- Delta to REJECT any UPDATE or DELETE operations at the protocol level.
-- This is not just a convention — it is enforced by the writer.
--
-- Append-only tables are ideal for:
--   - Audit logs (regulatory compliance)
--   - Event streams (immutable event sourcing)
--   - CDC landing tables (raw change captures)

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 4
ASSERT VALUE event_count = 10 WHERE event_type = 'AUTH'
ASSERT VALUE event_count = 7 WHERE event_type = 'DATA'
ASSERT VALUE event_count = 4 WHERE event_type = 'ADMIN'
ASSERT VALUE event_count = 4 WHERE event_type = 'SECURITY'
SELECT event_type,
       COUNT(*) AS event_count
FROM {{zone_name}}.delta_demos.audit_trail
GROUP BY event_type
ORDER BY event_count DESC;


-- ============================================================================
-- EXPLORE: Audit Trail — Security Event Timeline
-- ============================================================================
-- The append-only guarantee means every event is permanently recorded.
-- Let's trace a security incident: Dave's failed login attempts.

ASSERT ROW_COUNT = 4
SELECT id, event_type, actor, action, timestamp_utc
FROM {{zone_name}}.delta_demos.audit_trail
WHERE actor = 'dave@corp.com'
ORDER BY timestamp_utc;


-- ============================================================================
-- EXPLORE: Category Distribution After Feature Operations
-- ============================================================================
-- After the UPDATE (trial -> active) and DELETE (low-value items), the
-- category distribution has shifted. Hardware lost 3 items to deletion.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 3
ASSERT VALUE total_value = 3530.00 WHERE category = 'services'
ASSERT VALUE total_value = 3518.13 WHERE category = 'software'
ASSERT VALUE total_value = 3053.50 WHERE category = 'hardware'
SELECT category,
       COUNT(*) AS items,
       ROUND(SUM(value), 2) AS total_value
FROM {{zone_name}}.delta_demos.feature_demo
GROUP BY category
ORDER BY total_value DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify feature_demo has 27 rows after UPDATE and DELETE
ASSERT ROW_COUNT = 27
SELECT * FROM {{zone_name}}.delta_demos.feature_demo;

-- Verify no trial items remain after conversion to active
ASSERT VALUE trial_count = 0
SELECT COUNT(*) AS trial_count FROM {{zone_name}}.delta_demos.feature_demo WHERE status = 'trial';

-- Verify trial discount was applied (id=7 value should be 159.99)
ASSERT VALUE value = 159.99
SELECT value FROM {{zone_name}}.delta_demos.feature_demo WHERE id = 7;

-- Verify deleted items (ids 6, 9, 30) are gone
ASSERT VALUE deleted_count = 0
SELECT COUNT(*) AS deleted_count FROM {{zone_name}}.delta_demos.feature_demo WHERE id IN (6, 9, 30);

-- Verify hardware category has 10 items (13 original - 3 deleted: ids 6, 9, 30)
ASSERT VALUE hardware_count = 10
SELECT COUNT(*) AS hardware_count FROM {{zone_name}}.delta_demos.feature_demo WHERE category = 'hardware';

-- Verify audit trail has 25 events
ASSERT VALUE audit_trail_count = 25
SELECT COUNT(*) AS audit_trail_count FROM {{zone_name}}.delta_demos.audit_trail;

-- Verify 4 distinct event types in audit trail
ASSERT VALUE audit_event_types = 4
SELECT COUNT(DISTINCT event_type) AS audit_event_types FROM {{zone_name}}.delta_demos.audit_trail;

-- Verify Dave has 2 failed login attempts
ASSERT VALUE failed_login_count = 2
SELECT COUNT(*) AS failed_login_count FROM {{zone_name}}.delta_demos.audit_trail WHERE actor = 'dave@corp.com' AND action = 'LOGIN_FAILED';
