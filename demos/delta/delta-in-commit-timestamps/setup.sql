-- ============================================================================
-- Delta In-Commit Timestamps — Reliable Version Timing — Setup Script
-- ============================================================================
-- Demonstrates in-commit timestamps for reliable version timing:
--   - Monotonically increasing timestamps per table version
--   - Critical for TIMESTAMP AS OF time travel queries
--   - Accurate point-in-time recovery regardless of clock skew
--
-- Table created:
--   1. deployment_log — 40 deployment records across environments
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLE with TBLPROPERTIES ('delta.enableInCommitTimestamps'='true')
--   3. INSERT 20 rows — production deployments
--   4. DETECT SCHEMA + GRANT ADMIN
--   5. INSERT 10 rows — staging deployments
--   6. INSERT 10 rows — development deployments
--   7. UPDATE — 5 deployments status='rollback'
--   8. UPDATE — 3 deployments status='failed'
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: deployment_log — deployment tracking with in-commit timestamps
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.deployment_log (
    id                  INT,
    service             VARCHAR,
    environment         VARCHAR,
    version_str         VARCHAR,
    deployer            VARCHAR,
    status              VARCHAR,
    duration_seconds    INT,
    deploy_timestamp    VARCHAR,
    commit_hash         VARCHAR
) LOCATION '{{data_path}}/deployment_log'
TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true');


-- ============================================================================
-- STEP 3: INSERT batch 1 — 20 production deployments
-- ============================================================================
-- api-gateway: ids 1-4
INSERT INTO {{zone_name}}.delta_demos.deployment_log VALUES
    (1,  'api-gateway',     'production', 'v2.1.0', 'alice',   'success', 120, '2025-06-01 08:00:00', 'a1b2c3d4'),
    (2,  'api-gateway',     'production', 'v2.1.1', 'bob',     'success', 95,  '2025-06-03 10:30:00', 'b2c3d4e5'),
    (3,  'api-gateway',     'production', 'v2.2.0', 'alice',   'success', 140, '2025-06-08 14:00:00', 'c3d4e5f6'),
    (4,  'api-gateway',     'production', 'v2.2.1', 'charlie', 'success', 88,  '2025-06-12 09:15:00', 'd4e5f6g7');

-- user-service: ids 5-8
INSERT INTO {{zone_name}}.delta_demos.deployment_log VALUES
    (5,  'user-service',    'production', 'v3.0.0', 'bob',     'success', 200, '2025-06-02 11:00:00', 'e5f6g7h8'),
    (6,  'user-service',    'production', 'v3.0.1', 'alice',   'success', 110, '2025-06-05 16:45:00', 'f6g7h8i9'),
    (7,  'user-service',    'production', 'v3.1.0', 'charlie', 'success', 180, '2025-06-10 08:30:00', 'g7h8i9j0'),
    (8,  'user-service',    'production', 'v3.1.1', 'bob',     'success', 105, '2025-06-14 13:00:00', 'h8i9j0k1');

-- payment-engine: ids 9-12
INSERT INTO {{zone_name}}.delta_demos.deployment_log VALUES
    (9,  'payment-engine',  'production', 'v1.5.0', 'charlie', 'success', 300, '2025-06-01 20:00:00', 'i9j0k1l2'),
    (10, 'payment-engine',  'production', 'v1.5.1', 'alice',   'success', 150, '2025-06-04 07:30:00', 'j0k1l2m3'),
    (11, 'payment-engine',  'production', 'v1.6.0', 'bob',     'success', 280, '2025-06-09 19:00:00', 'k1l2m3n4'),
    (12, 'payment-engine',  'production', 'v1.6.1', 'charlie', 'success', 130, '2025-06-13 10:45:00', 'l2m3n4o5');

-- notification-hub: ids 13-16
INSERT INTO {{zone_name}}.delta_demos.deployment_log VALUES
    (13, 'notification-hub', 'production', 'v4.0.0', 'alice',   'success', 160, '2025-06-02 15:30:00', 'm3n4o5p6'),
    (14, 'notification-hub', 'production', 'v4.0.1', 'bob',     'success', 75,  '2025-06-06 09:00:00', 'n4o5p6q7'),
    (15, 'notification-hub', 'production', 'v4.1.0', 'charlie', 'success', 190, '2025-06-11 12:15:00', 'o5p6q7r8'),
    (16, 'notification-hub', 'production', 'v4.1.1', 'alice',   'success', 85,  '2025-06-15 08:00:00', 'p6q7r8s9');

-- search-indexer: ids 17-20
INSERT INTO {{zone_name}}.delta_demos.deployment_log VALUES
    (17, 'search-indexer',  'production', 'v2.0.0', 'bob',     'success', 240, '2025-06-03 18:00:00', 'q7r8s9t0'),
    (18, 'search-indexer',  'production', 'v2.0.1', 'charlie', 'success', 100, '2025-06-07 11:30:00', 'r8s9t0u1'),
    (19, 'search-indexer',  'production', 'v2.1.0', 'alice',   'success', 210, '2025-06-12 16:00:00', 's9t0u1v2'),
    (20, 'search-indexer',  'production', 'v2.1.1', 'bob',     'success', 115, '2025-06-15 14:30:00', 't0u1v2w3');

DETECT SCHEMA FOR TABLE {{zone_name}}.delta_demos.deployment_log;
GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.deployment_log TO USER {{current_user}};


-- ============================================================================
-- STEP 5: INSERT batch 2 — 10 staging deployments
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.deployment_log VALUES
    (21, 'api-gateway',     'staging', 'v2.3.0-rc1', 'alice',   'success', 90,  '2025-06-14 08:00:00', 'u1v2w3x4'),
    (22, 'api-gateway',     'staging', 'v2.3.0-rc2', 'bob',     'success', 85,  '2025-06-15 09:00:00', 'v2w3x4y5'),
    (23, 'user-service',    'staging', 'v3.2.0-rc1', 'charlie', 'success', 170, '2025-06-14 11:30:00', 'w3x4y5z6'),
    (24, 'user-service',    'staging', 'v3.2.0-rc2', 'alice',   'success', 155, '2025-06-15 13:00:00', 'x4y5z6a7'),
    (25, 'payment-engine',  'staging', 'v1.7.0-rc1', 'bob',     'success', 260, '2025-06-14 15:00:00', 'y5z6a7b8'),
    (26, 'payment-engine',  'staging', 'v1.7.0-rc2', 'charlie', 'success', 240, '2025-06-15 16:30:00', 'z6a7b8c9'),
    (27, 'notification-hub', 'staging', 'v4.2.0-rc1', 'alice',   'success', 140, '2025-06-14 17:00:00', 'a7b8c9d0'),
    (28, 'notification-hub', 'staging', 'v4.2.0-rc2', 'bob',     'success', 125, '2025-06-15 18:00:00', 'b8c9d0e1'),
    (29, 'search-indexer',  'staging', 'v2.2.0-rc1', 'charlie', 'success', 200, '2025-06-14 19:30:00', 'c9d0e1f2'),
    (30, 'search-indexer',  'staging', 'v2.2.0-rc2', 'alice',   'success', 185, '2025-06-15 20:00:00', 'd0e1f2g3');


-- ============================================================================
-- STEP 6: INSERT batch 3 — 10 development deployments
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.deployment_log VALUES
    (31, 'api-gateway',     'development', 'v2.4.0-dev', 'alice',   'success', 60,  '2025-06-15 07:00:00', 'e1f2g3h4'),
    (32, 'api-gateway',     'development', 'v2.4.1-dev', 'bob',     'success', 55,  '2025-06-15 07:30:00', 'f2g3h4i5'),
    (33, 'user-service',    'development', 'v3.3.0-dev', 'charlie', 'success', 130, '2025-06-15 08:00:00', 'g3h4i5j6'),
    (34, 'user-service',    'development', 'v3.3.1-dev', 'alice',   'success', 120, '2025-06-15 08:30:00', 'h4i5j6k7'),
    (35, 'payment-engine',  'development', 'v1.8.0-dev', 'bob',     'success', 210, '2025-06-15 09:00:00', 'i5j6k7l8'),
    (36, 'payment-engine',  'development', 'v1.8.1-dev', 'charlie', 'success', 195, '2025-06-15 09:30:00', 'j6k7l8m9'),
    (37, 'notification-hub', 'development', 'v4.3.0-dev', 'alice',   'success', 100, '2025-06-15 10:00:00', 'k7l8m9n0'),
    (38, 'notification-hub', 'development', 'v4.3.1-dev', 'bob',     'success', 90,  '2025-06-15 10:30:00', 'l8m9n0o1'),
    (39, 'search-indexer',  'development', 'v2.3.0-dev', 'charlie', 'success', 150, '2025-06-15 11:00:00', 'm9n0o1p2'),
    (40, 'search-indexer',  'development', 'v2.3.1-dev', 'alice',   'success', 140, '2025-06-15 11:30:00', 'n0o1p2q3');


-- ============================================================================
-- STEP 7: UPDATE — 5 deployments status='rollback'
-- ============================================================================
-- Rollback ids: 3, 11, 15, 23, 29
UPDATE {{zone_name}}.delta_demos.deployment_log
SET status = 'rollback'
WHERE id = 3;

UPDATE {{zone_name}}.delta_demos.deployment_log
SET status = 'rollback'
WHERE id = 11;

UPDATE {{zone_name}}.delta_demos.deployment_log
SET status = 'rollback'
WHERE id = 15;

UPDATE {{zone_name}}.delta_demos.deployment_log
SET status = 'rollback'
WHERE id = 23;

UPDATE {{zone_name}}.delta_demos.deployment_log
SET status = 'rollback'
WHERE id = 29;


-- ============================================================================
-- STEP 8: UPDATE — 3 deployments status='failed'
-- ============================================================================
-- Failed ids: 7, 25, 35
UPDATE {{zone_name}}.delta_demos.deployment_log
SET status = 'failed'
WHERE id = 7;

UPDATE {{zone_name}}.delta_demos.deployment_log
SET status = 'failed'
WHERE id = 25;

UPDATE {{zone_name}}.delta_demos.deployment_log
SET status = 'failed'
WHERE id = 35;
