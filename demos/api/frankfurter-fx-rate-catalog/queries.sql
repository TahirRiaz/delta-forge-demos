-- ============================================================================
-- Demo: Frankfurter FX Rate Catalog, Queries
-- ============================================================================
-- This file is where the two API endpoints are actually exercised. The
-- whole API surface lives here, registry inspection (post-ALTER),
-- DESCRIBE on the renamed leaf, both INVOKE calls, per-endpoint run
-- audits, schema detection, and the bronze->silver promotion, followed
-- by the assertions that prove the multi-endpoint + ALTER flow worked.
--
-- Validates the multi-endpoint + ALTER flow:
--   - Exactly 2 rows, one per endpoint (latest_eur_basket +
--     euro_launch_day_rates).
--   - 2 distinct base currencies: EUR (the latest endpoint) and USD
--     (the ALTERed endpoint, post-RENAME).
--   - The euro_launch_day_rates endpoint returned the canonical
--     1999-01-04 date, proving `ALTER ... SET URL` correctly
--     substituted the historical date into the URL template.
--   - base_amount is always 1.0 (Frankfurter's default "1 unit of base"
--     convention) for both rows.
--   - Rate dates are plausible (not in the future) and distinct.
--
-- Stability: 1999-01-04 is a fixed historical anchor, never moves.
-- The latest rate date is "today or very recent" (Frankfurter returns
-- the most recent ECB publication day). All assertions here are
-- boundary-safe; no specific exchange-rate values are asserted because
-- they drift daily.
-- ============================================================================

-- ============================================================================
-- API surface, calling the endpoints from SQL
-- ============================================================================

-- Confirm both endpoints exist after the ALTER + RENAME, historical_stub
-- should no longer appear, euro_launch_day_rates should appear in its
-- place, latest_eur_basket is unchanged.
SHOW API ENDPOINTS IN CONNECTION {{zone_name}}.frankfurter_fx;

-- Inspect the renamed endpoint's post-ALTER URL, options, and last run
-- status before the INVOKE.
DESCRIBE API ENDPOINT {{zone_name}}.frankfurter_fx.euro_launch_day_rates;

-- INVOKE both endpoints. Each writes to its own subfolder under
-- frankfurter_fx/..., the bronze table picks up both via recursive scan.
INVOKE API ENDPOINT {{zone_name}}.frankfurter_fx.latest_eur_basket;
INVOKE API ENDPOINT {{zone_name}}.frankfurter_fx.euro_launch_day_rates;

-- Per-endpoint run audit. Each endpoint has its own run history,
-- post-INVOKE both must show at least one 'success' row.
SHOW API ENDPOINT RUNS {{zone_name}}.frankfurter_fx.latest_eur_basket LIMIT 5;
SHOW API ENDPOINT RUNS {{zone_name}}.frankfurter_fx.euro_launch_day_rates LIMIT 5;

-- Resolve the bronze schema from the freshly written JSON pages.
DETECT SCHEMA FOR TABLE {{zone_name}}.frankfurter_fx.fx_rates_bronze;

-- Bronze -> silver promotion with parsed DATE.
INSERT INTO {{zone_name}}.frankfurter_fx.fx_rates_silver
SELECT
    base_currency,
    CAST(rate_date AS DATE)   AS rate_date,
    CAST(base_amount AS DOUBLE) AS base_amount
FROM {{zone_name}}.frankfurter_fx.fx_rates_bronze;

-- ============================================================================
-- Query 1: Endpoint Fan-In, 2 endpoints -> 2 rows
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE endpoint_rows = 2
SELECT COUNT(*) AS endpoint_rows
FROM {{zone_name}}.frankfurter_fx.fx_rates_bronze;

-- ============================================================================
-- Query 2: Base-Currency Split, EUR latest + USD historical
-- ============================================================================
-- latest_eur_basket queries `from=EUR`; the renamed
-- euro_launch_day_rates queries `from=USD`. Exactly one row each.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_bases = 2
ASSERT VALUE eur_rows = 1
ASSERT VALUE usd_rows = 1
SELECT
    COUNT(DISTINCT base_currency)                              AS distinct_bases,
    SUM(CASE WHEN base_currency = 'EUR' THEN 1 ELSE 0 END)     AS eur_rows,
    SUM(CASE WHEN base_currency = 'USD' THEN 1 ELSE 0 END)     AS usd_rows
FROM {{zone_name}}.frankfurter_fx.fx_rates_bronze;

-- ============================================================================
-- Query 3: ALTER SET URL Round-Trip, 1999-01-04 is present
-- ============================================================================
-- The ALTER ... SET URL '/v1/1999-01-04?...' swapped the endpoint's
-- URL from a latest-date query to a fixed historical date. If the
-- ALTER didn't take effect, the response would contain a recent date
-- instead. 1 match here proves the URL change made it to the wire.

ASSERT ROW_COUNT = 1
ASSERT VALUE euro_launch = 1
SELECT SUM(CASE WHEN rate_date = DATE '1999-01-04' THEN 1 ELSE 0 END) AS euro_launch
FROM {{zone_name}}.frankfurter_fx.fx_rates_silver;

-- ============================================================================
-- Query 4: Base-Amount Convention, "1 unit of base"
-- ============================================================================
-- Frankfurter normalizes to "1 unit of base currency" unless amount= is
-- overridden. Both rows must have base_amount = 1.0, an easy sanity
-- check on the DOUBLE cast.

ASSERT ROW_COUNT = 1
ASSERT VALUE base_amount_one = 2
SELECT SUM(CASE WHEN base_amount = 1.0 THEN 1 ELSE 0 END) AS base_amount_one
FROM {{zone_name}}.frankfurter_fx.fx_rates_silver;

-- ============================================================================
-- Query 5: Date Sanity, not in the future, 2 distinct days
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE latest_not_future = 1
ASSERT VALUE dates_distinct = 1
SELECT
    CASE WHEN MAX(rate_date) <= CURRENT_DATE THEN 1 ELSE 0 END AS latest_not_future,
    CASE WHEN COUNT(DISTINCT rate_date) = 2 THEN 1 ELSE 0 END  AS dates_distinct
FROM {{zone_name}}.frankfurter_fx.fx_rates_silver;

-- ============================================================================
-- Query 6: Silver Delta History, v0 schema + v1 INSERT
-- ============================================================================

ASSERT ROW_COUNT >= 2
DESCRIBE HISTORY {{zone_name}}.frankfurter_fx.fx_rates_silver;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_endpoints_landed = 2
ASSERT VALUE both_bases_present = 1
ASSERT VALUE euro_anchor_present = 1
ASSERT VALUE bronze_silver_parity = 1
SELECT
    COUNT(*)                                                                                AS total_endpoints_landed,
    CASE WHEN COUNT(DISTINCT base_currency) = 2 THEN 1 ELSE 0 END                           AS both_bases_present,
    CASE WHEN SUM(CASE WHEN rate_date = DATE '1999-01-04' THEN 1 ELSE 0 END) = 1
         THEN 1 ELSE 0 END                                                                  AS euro_anchor_present,
    CASE WHEN COUNT(*) = (SELECT COUNT(*) FROM {{zone_name}}.frankfurter_fx.fx_rates_bronze)
         THEN 1 ELSE 0 END                                                                  AS bronze_silver_parity
FROM {{zone_name}}.frankfurter_fx.fx_rates_silver;
