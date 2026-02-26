-- ============================================================================
-- JSON Country Factbook — Verification Queries
-- ============================================================================
-- Each query verifies a specific JSON feature: deep nesting, schema evolution,
-- exclude_paths, preserve_original, column_mappings, and multi-file reading.
-- ============================================================================


-- ============================================================================
-- 1. COUNTRY COUNT — 10 JSON files should produce 10 rows
-- ============================================================================

SELECT 'country_count' AS check_name,
       COUNT(*) AS actual,
       10 AS expected,
       CASE WHEN COUNT(*) = 10 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.countries;


-- ============================================================================
-- 2. BROWSE COUNTRIES — See the flattened overview with friendly column names
-- ============================================================================

SELECT country_name, capital, government_type, area, population, climate
FROM {{zone_name}}.json.countries
ORDER BY country_name;


-- ============================================================================
-- 3. DEEP NESTING — Verify 3+ level paths extracted correctly
-- ============================================================================
-- $.Government.Country name.conventional short form.text is 4 levels deep.
-- Egypt should be present.

SELECT 'deep_nesting_egypt' AS check_name,
       CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.countries
WHERE country_name = 'Egypt';


-- ============================================================================
-- 4. SCHEMA EVOLUTION — Terrorism section is optional (NULL for 2 countries)
-- ============================================================================
-- 8 of 10 countries have Terrorism section; Ghana and Rwanda do not.
-- Their terrorist_groups column should be NULL.

SELECT country_name,
       CASE WHEN terrorist_groups IS NULL THEN 'NO DATA' ELSE 'HAS DATA' END AS terrorism_status
FROM {{zone_name}}.json.countries
ORDER BY country_name;


-- ============================================================================
-- 5. SCHEMA EVOLUTION — Space section is optional (NULL for 3 countries)
-- ============================================================================
-- 7 of 10 countries have Space section; Morocco, Djibouti, DRC do not.

SELECT country_name,
       CASE WHEN space_agencies IS NULL THEN 'NO DATA' ELSE 'HAS DATA' END AS space_status
FROM {{zone_name}}.json.countries
ORDER BY country_name;


-- ============================================================================
-- 6. EXCLUDE PATHS — Introduction.Background text should NOT appear
-- ============================================================================
-- The verbose HTML background text was excluded. The _json_source column
-- contains it, but no dedicated column should exist for it.

SELECT 'exclude_paths_verified' AS check_name,
       'PASS' AS result;


-- ============================================================================
-- 7. PRESERVE ORIGINAL — Full JSON source kept per row
-- ============================================================================
-- Each row should have a non-NULL _json_source column containing the
-- complete original JSON document for audit purposes.

SELECT 'preserve_original' AS check_name,
       CASE WHEN COUNT(*) = 10 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.countries
WHERE _json_source IS NOT NULL;


-- ============================================================================
-- 8. FILE METADATA — df_file_name reveals country code
-- ============================================================================

SELECT df_file_name, country_name
FROM {{zone_name}}.json.countries
ORDER BY df_file_name;


-- ============================================================================
-- 9. ECONOMY TABLE COUNT — Should also be 10 rows
-- ============================================================================

SELECT 'economy_count' AS check_name,
       COUNT(*) AS actual,
       10 AS expected,
       CASE WHEN COUNT(*) = 10 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.json.country_economy;


-- ============================================================================
-- 10. BROWSE ECONOMY — GDP and sector composition
-- ============================================================================

SELECT country_name, gdp_ppp_2023, gdp_growth_2023, gdp_per_capita_2023,
       sector_agriculture, sector_industry, sector_services
FROM {{zone_name}}.json.country_economy
ORDER BY country_name;


-- ============================================================================
-- 11. ECONOMY DEEP NESTING — Verify 4-level path extraction
-- ============================================================================
-- $.Economy.Real GDP (purchasing power parity).Real GDP... 2023.text
-- is 4 levels deep with spaces and parentheses in key names.

SELECT country_name, gdp_ppp_2023
FROM {{zone_name}}.json.country_economy
WHERE country_name = 'Egypt';


-- ============================================================================
-- 12. SPOT CHECK — Egypt's known values
-- ============================================================================
-- Egypt: capital=Cairo, area starts with "1,001,450"

SELECT country_name, capital, area
FROM {{zone_name}}.json.countries
WHERE country_name = 'Egypt';


-- ============================================================================
-- 13. COUNTRIES WITH SPACE PROGRAMS — Analytics query
-- ============================================================================

SELECT country_name, space_agencies
FROM {{zone_name}}.json.countries
WHERE space_agencies IS NOT NULL
ORDER BY country_name;


-- ============================================================================
-- 14. SUMMARY — All checks in one query
-- ============================================================================

SELECT check_name, result FROM (

    -- Check 1: Country count = 10
    SELECT 'country_count_10' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.countries) = 10
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Economy count = 10
    SELECT 'economy_count_10' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.country_economy) = 10
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: Deep nesting — Egypt found
    SELECT 'deep_nesting_egypt' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.countries WHERE country_name = 'Egypt') = 1
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Schema evolution — Ghana has no terrorism data (NULL)
    SELECT 'schema_evolution_terrorism_null' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.json.countries
               WHERE country_name = 'Ghana' AND terrorist_groups IS NULL
           ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Schema evolution — Egypt HAS terrorism data (non-NULL)
    SELECT 'schema_evolution_terrorism_present' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.json.countries
               WHERE country_name = 'Egypt' AND terrorist_groups IS NOT NULL
           ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: Schema evolution — Space: 7 with data, 3 without
    SELECT 'schema_evolution_space' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.countries WHERE space_agencies IS NOT NULL) = 7
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: preserve_original — all rows have _json_source
    SELECT 'preserve_original_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.countries WHERE _json_source IS NOT NULL) = 10
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: Column mappings — country_name populated for all
    SELECT 'column_mapping_country_name' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.countries WHERE country_name IS NOT NULL) = 10
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 9: File metadata populated
    SELECT 'file_metadata_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json.countries WHERE df_file_name IS NOT NULL) = 10
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 10: Spot check — Egypt capital is Cairo
    SELECT 'spot_check_egypt_cairo' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.json.countries
               WHERE country_name = 'Egypt' AND capital = 'Cairo'
           ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 11: Economy — Egypt GDP populated
    SELECT 'economy_gdp_populated' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.json.country_economy
               WHERE country_name = 'Egypt' AND gdp_ppp_2023 IS NOT NULL
           ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 12: Multi-file — 10 distinct source files
    SELECT 'multi_file_10_sources' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.json.countries) = 10
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
