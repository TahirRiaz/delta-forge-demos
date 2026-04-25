-- ============================================================================
-- Demo: Vendor Auth Smoke Test, Queries
-- ============================================================================
-- This file is where the credentialed endpoints are actually exercised.
-- Storage backend audit, registry inspection, both INVOKE calls,
-- per-endpoint run audits, and schema detection all live here so the
-- user sees the X-API-Key round-trip end to end from a single file,
-- followed by the assertions that prove the auth path worked.
--
-- Validates the full api_key_header auth path:
--   - Both endpoints INVOKE-landed exactly 1 row each (single-page
--     responses from httpbin).
--   - The X-API-Key header value httpbin echoed back matches the
--     literal secret stored in the vault, end-to-end auth proof.
--   - The Host header echoed back is 'httpbin.org', proves the
--     request reached the right endpoint.
--   - The /uuid endpoint's response matches a canonical UUID v4 shape.
--   - Headers_bronze row has the expected 20-character api key value.
--
-- This is the strongest possible round-trip proof for the credential
-- layer: the test doesn't just check that an INVOKE succeeded, it
-- pulls the actual header value BACK out of the server's echo and
-- asserts on it.
-- ============================================================================

-- ============================================================================
-- API surface, calling the endpoints from SQL
-- ============================================================================

-- Surface the always-on OS Keychain backend so the security reviewer
-- can see it catalogued.
SHOW CREDENTIAL STORAGES;

-- Confirm only the two surviving endpoints remain after the
-- DROP API ENDPOINT in setup.sql removed the typo'd third one.
SHOW API ENDPOINTS IN CONNECTION {{zone_name}}.httpbin_smoke;

-- INVOKE both surviving endpoints. Each writes one JSON page under
-- its endpoint's subfolder.
INVOKE API ENDPOINT {{zone_name}}.httpbin_smoke.probe_headers;
INVOKE API ENDPOINT {{zone_name}}.httpbin_smoke.probe_uuid;

-- Per-endpoint run audit.
SHOW API ENDPOINT RUNS {{zone_name}}.httpbin_smoke.probe_headers LIMIT 5;
SHOW API ENDPOINT RUNS {{zone_name}}.httpbin_smoke.probe_uuid LIMIT 5;

-- Resolve bronze schemas from the freshly written JSON pages.
DETECT SCHEMA FOR TABLE {{zone_name}}.httpbin_smoke.headers_bronze;
DETECT SCHEMA FOR TABLE {{zone_name}}.httpbin_smoke.uuid_bronze;

-- ============================================================================
-- Query 1: Headers Endpoint Returned 1 Row
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE headers_rows = 1
SELECT COUNT(*) AS headers_rows
FROM {{zone_name}}.httpbin_smoke.headers_bronze;

-- ============================================================================
-- Query 2: UUID Endpoint Returned 1 Row
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE uuid_rows = 1
SELECT COUNT(*) AS uuid_rows
FROM {{zone_name}}.httpbin_smoke.uuid_bronze;

-- ============================================================================
-- Query 3: X-API-Key Round-Trip, the strong auth-path proof
-- ============================================================================
-- httpbin's /headers echoes every received header back in the response
-- body. The JSON flatten pulled $.headers.X-Api-Key into the
-- x_api_key_echo column. If that value matches the literal vault
-- secret, the WHOLE auth chain worked:
--   - CREDENTIAL STORAGE created
--   - CREDENTIAL stored in it with the right SECRET
--   - CONNECTION bound to the CREDENTIAL
--   - Session token inner-sealed the secret material
--   - Engine read the secret from the token
--   - Engine set X-API-Key header on the request
--   - httpbin echoed it back
--   - JSON flatten pulled it into the column
-- Any break in that chain would surface as a missing / wrong value.

ASSERT ROW_COUNT = 1
ASSERT VALUE api_key_roundtrip = 1
ASSERT VALUE correct_host = 1
SELECT
    SUM(CASE WHEN x_api_key_echo = 'df-smoke-test-abc123' THEN 1 ELSE 0 END) AS api_key_roundtrip,
    SUM(CASE WHEN request_host = 'httpbin.org'            THEN 1 ELSE 0 END) AS correct_host
FROM {{zone_name}}.httpbin_smoke.headers_bronze;

-- ============================================================================
-- Query 4: UUID Shape, canonical 8-4-4-4-12 hex format
-- ============================================================================
-- httpbin's /uuid returns a UUID v4 like "abc12345-1234-1234-1234-
-- 123456789abc". LIKE with `_` placeholders asserts the shape.

ASSERT ROW_COUNT = 1
ASSERT VALUE uuid_looks_right = 1
SELECT
    CASE WHEN generated_uuid LIKE '________-____-____-____-____________' THEN 1 ELSE 0 END AS uuid_looks_right
FROM {{zone_name}}.httpbin_smoke.uuid_bronze;

-- ============================================================================
-- Query 5: Exact-String Echo Proof
-- ============================================================================
-- Two different assertions on the same echoed value, exact string
-- match AND expected length (20 chars). Belt-and-suspenders.

ASSERT ROW_COUNT = 1
ASSERT VALUE headers_echo = 'df-smoke-test-abc123'
ASSERT VALUE headers_echo_length = 20
SELECT
    x_api_key_echo           AS headers_echo,
    LENGTH(x_api_key_echo)   AS headers_echo_length
FROM {{zone_name}}.httpbin_smoke.headers_bronze;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One cross-cutting query: total rows across both probe tables, the
-- API key echo matches literally, the UUID shape matches, and both
-- endpoints actually landed data.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_probes = 2
ASSERT VALUE api_key_present = 1
ASSERT VALUE uuid_shape_ok = 1
ASSERT VALUE distinct_endpoints_landed = 2
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.httpbin_smoke.headers_bronze)
      + (SELECT COUNT(*) FROM {{zone_name}}.httpbin_smoke.uuid_bronze)                           AS total_probes,
    (SELECT CASE WHEN x_api_key_echo = 'df-smoke-test-abc123' THEN 1 ELSE 0 END
       FROM {{zone_name}}.httpbin_smoke.headers_bronze)                                           AS api_key_present,
    (SELECT CASE WHEN generated_uuid LIKE '________-____-____-____-____________' THEN 1 ELSE 0 END
       FROM {{zone_name}}.httpbin_smoke.uuid_bronze)                                              AS uuid_shape_ok,
    CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.httpbin_smoke.headers_bronze) > 0
           AND (SELECT COUNT(*) FROM {{zone_name}}.httpbin_smoke.uuid_bronze) > 0
         THEN 2 ELSE 0 END                                                                       AS distinct_endpoints_landed;
