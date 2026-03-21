-- ============================================================================
-- Delta In-Commit Timestamps — Educational Queries
-- ============================================================================
-- WHAT: In-commit timestamps embed a monotonically increasing timestamp
--       directly into each Delta transaction log commit entry.
-- WHY:  Without this feature, Delta derives version timestamps from file
--       modification times, which can be unreliable when multiple writers
--       have clock skew. This breaks TIMESTAMP AS OF time travel queries.
-- HOW:  When 'delta.enableInCommitTimestamps' = 'true', each commit action
--       in the _delta_log JSON file includes a reliable timestamp field.
--       This guarantees monotonicity regardless of writer clock differences.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Deployment Log Overview
-- ============================================================================
-- The deployment_log table was created with in-commit timestamps enabled.
-- It tracks deployments across 3 environments with various outcomes.
-- Each batch INSERT and UPDATE created a new version in the transaction
-- log, each with a reliable in-commit timestamp.

-- Verify deployment counts per environment
ASSERT VALUE deployment_count = 10 WHERE environment = 'development'
ASSERT VALUE deployment_count = 20 WHERE environment = 'production'
ASSERT VALUE deployment_count = 10 WHERE environment = 'staging'
ASSERT ROW_COUNT = 3
SELECT environment,
       COUNT(*) AS deployment_count,
       COUNT(*) FILTER (WHERE status = 'success') AS successful,
       COUNT(*) FILTER (WHERE status = 'rollback') AS rolled_back,
       COUNT(*) FILTER (WHERE status = 'failed') AS failed
FROM {{zone_name}}.delta_demos.deployment_log
GROUP BY environment
ORDER BY environment;


-- ============================================================================
-- LEARN: Why In-Commit Timestamps Matter for Time Travel
-- ============================================================================
-- This table was built through multiple INSERT and UPDATE operations,
-- each creating a new Delta version. The in-commit timestamp on each
-- version means you could use TIMESTAMP AS OF to query the table at
-- any point in its history — for example, seeing the state before
-- rollbacks were applied or before staging deployments were added.
--
-- The setup created these versions:
--   V0: CREATE TABLE
--   V1-V5: INSERT production batches (4 services x 4 deploys + 1 batch)
--   V6: INSERT staging batch
--   V7: INSERT development batch
--   V8-V15: UPDATE operations (rollbacks and failures)
--
-- Let's examine the services and their deployment history:

ASSERT ROW_COUNT = 5
ASSERT VALUE total_deploys = 8 WHERE service = 'payment-engine'
ASSERT VALUE avg_duration_sec = 221 WHERE service = 'payment-engine'
ASSERT VALUE total_deploys = 8 WHERE service = 'api-gateway'
ASSERT VALUE avg_duration_sec = 92 WHERE service = 'api-gateway'
SELECT service,
       COUNT(*) AS total_deploys,
       COUNT(DISTINCT environment) AS environments_deployed_to,
       ROUND(AVG(duration_seconds), 0) AS avg_duration_sec,
       MIN(deploy_timestamp) AS first_deploy,
       MAX(deploy_timestamp) AS last_deploy
FROM {{zone_name}}.delta_demos.deployment_log
GROUP BY service
ORDER BY service;


-- ============================================================================
-- EXPLORE: Deployment Outcomes Across Environments
-- ============================================================================
-- Rollbacks and failures were applied via UPDATE after initial inserts.
-- In the transaction log, each UPDATE created a new version with its
-- own in-commit timestamp, enabling precise identification of when
-- each status change occurred.

-- Verify 5 rollbacks + 3 failures = 8 non-success deployments
ASSERT ROW_COUNT = 8
SELECT id, service, environment, version_str, status, duration_seconds
FROM {{zone_name}}.delta_demos.deployment_log
WHERE status IN ('rollback', 'failed')
ORDER BY id;


-- ============================================================================
-- LEARN: Deployer Activity and Version Correlation
-- ============================================================================
-- Each deployer's commits correspond to specific Delta versions.
-- With in-commit timestamps, you could audit exactly when each
-- deployer's changes were committed to the table, independent of
-- the deploy_timestamp field in the data itself.

ASSERT ROW_COUNT = 3
ASSERT VALUE deployments = 15 WHERE deployer = 'alice'
ASSERT VALUE successes = 14 WHERE deployer = 'alice'
ASSERT VALUE deployments = 13 WHERE deployer = 'bob'
ASSERT VALUE issues = 3 WHERE deployer = 'bob'
SELECT deployer,
       COUNT(*) AS deployments,
       COUNT(*) FILTER (WHERE status = 'success') AS successes,
       COUNT(*) FILTER (WHERE status != 'success') AS issues,
       COUNT(DISTINCT service) AS services_touched
FROM {{zone_name}}.delta_demos.deployment_log
GROUP BY deployer
ORDER BY deployments DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_row_count: 40 deployments total
ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.delta_demos.deployment_log;

-- Verify production_count: 20 production deployments
ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.deployment_log WHERE environment = 'production';

-- Verify staging_count: 10 staging deployments
ASSERT VALUE cnt = 10
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.deployment_log WHERE environment = 'staging';

-- Verify development_count: 10 development deployments
ASSERT VALUE cnt = 10
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.deployment_log WHERE environment = 'development';

-- Verify rollback_count: 5 deployments rolled back
ASSERT VALUE cnt = 5
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.deployment_log WHERE status = 'rollback';

-- Verify failed_count: 3 deployments failed
ASSERT VALUE cnt = 3
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.deployment_log WHERE status = 'failed';

-- Verify success_count: 32 successful deployments
ASSERT VALUE cnt = 32
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.deployment_log WHERE status = 'success';

-- Verify distinct_services: 5 different services deployed
ASSERT VALUE cnt = 5
SELECT COUNT(DISTINCT service) AS cnt FROM {{zone_name}}.delta_demos.deployment_log;
