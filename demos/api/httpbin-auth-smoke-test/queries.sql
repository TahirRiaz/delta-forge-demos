-- ============================================================================
-- Demo: Vendor Auth Smoke Test, Queries
-- ============================================================================
-- API demo assertion policy: only ASSERT ROW_COUNT > 0 is used.
--
-- Block ordering note: each INVOKE is in its own block with no bronze
-- references. The planner pre-registers external tables across the
-- whole script and JSON registration fails on empty directories, so any
-- block that touches headers_bronze / uuid_bronze must run after the
-- corresponding INVOKE has populated its directory.
-- ============================================================================

-- ============================================================================
-- Block 1: storage backend audit
-- ============================================================================

SHOW CREDENTIAL STORAGES;

-- ============================================================================
-- Block 2: registry inspection
-- ============================================================================

SHOW API ENDPOINTS IN CONNECTION {{zone_name}}.httpbin_smoke;

-- ============================================================================
-- Block 3: INVOKE both endpoints (no bronze references)
-- ============================================================================

INVOKE API ENDPOINT {{zone_name}}.httpbin_smoke.probe_headers;
INVOKE API ENDPOINT {{zone_name}}.httpbin_smoke.probe_uuid;

-- ============================================================================
-- Block 4: per-endpoint run audit
-- ============================================================================

SHOW API ENDPOINT RUNS {{zone_name}}.httpbin_smoke.probe_headers LIMIT 5;

-- ============================================================================
-- Block 5: per-endpoint run audit (uuid)
-- ============================================================================

SHOW API ENDPOINT RUNS {{zone_name}}.httpbin_smoke.probe_uuid LIMIT 5;

-- ============================================================================
-- Block 6: detect bronze schemas
-- ============================================================================

DETECT SCHEMA FOR TABLE {{zone_name}}.httpbin_smoke.headers_bronze;

-- ============================================================================
-- Block 7: detect uuid schema
-- ============================================================================

DETECT SCHEMA FOR TABLE {{zone_name}}.httpbin_smoke.uuid_bronze;

-- ============================================================================
-- Block 8: headers endpoint response
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    x_api_key_echo,
    request_host
FROM {{zone_name}}.httpbin_smoke.headers_bronze;

-- ============================================================================
-- Block 9: UUID endpoint response
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    generated_uuid
FROM {{zone_name}}.httpbin_smoke.uuid_bronze;
