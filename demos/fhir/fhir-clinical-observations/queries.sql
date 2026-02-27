-- ============================================================================
-- FHIR Clinical Observations — Queries
-- ============================================================================
-- Explore FHIR R5 Observation resources covering vital signs, lab results,
-- and clinical assessments. These queries demonstrate how Delta Forge
-- flattens complex FHIR clinical data into analyst-friendly SQL tables.
-- ============================================================================


-- ============================================================================
-- 1. BULK HEART RATE OVERVIEW
-- ============================================================================
-- The bulk NDJSON file contains 100 heart rate observations (LOINC code
-- 8867-4). Each record links to a patient via the "subject" reference and
-- contains a valueQuantity with the heart rate in beats/minute. This is the
-- most common pattern in FHIR bulk data exports.

SELECT observation_id, status, subject, effective_date, value_quantity
FROM {{zone_name}}.fhir.observations_bulk
ORDER BY effective_date
LIMIT 15;


-- ============================================================================
-- 2. HEART RATE STATISTICS — Population-level vital signs analytics
-- ============================================================================
-- From 100 heart rate observations, compute basic statistics. In clinical
-- practice, heart rates typically range 60-100 bpm for adults at rest.
-- Values outside this range may indicate tachycardia (>100) or bradycardia
-- (<60). The FHIR valueQuantity stores value and unit together.

SELECT COUNT(*) AS total_readings,
       MIN(df_row_number) AS first_record,
       MAX(df_row_number) AS last_record
FROM {{zone_name}}.fhir.observations_bulk;


-- ============================================================================
-- 3. CLINICAL OBSERVATIONS OVERVIEW — All 14 individual observations
-- ============================================================================
-- Each file represents a different clinical measurement type. Notice how
-- the "code" column contains FHIR CodeableConcept JSON with LOINC/SNOMED
-- coding systems. The observation_id uniquely identifies each measurement,
-- and status indicates whether the observation is "final", "preliminary",
-- "amended", or "cancelled".

SELECT observation_id, status, code, effective_date, value_quantity
FROM {{zone_name}}.fhir.observations_clinical
ORDER BY observation_id;


-- ============================================================================
-- 4. SCHEMA EVOLUTION ACROSS OBSERVATION TYPES
-- ============================================================================
-- Different FHIR Observation types populate different fields. Vital signs
-- use valueQuantity (a single number + unit), blood pressure uses component[]
-- (systolic + diastolic as sub-observations), and Glasgow Coma Scale also
-- uses component[]. Lab results often include referenceRange and
-- interpretation. This query shows which fields are populated per observation.

SELECT observation_id,
       CASE WHEN value_quantity IS NOT NULL THEN 'Y' ELSE '-' END AS has_value,
       CASE WHEN value_string IS NOT NULL THEN 'Y' ELSE '-' END AS has_string,
       CASE WHEN component IS NOT NULL THEN 'Y' ELSE '-' END AS has_component,
       CASE WHEN reference_range IS NOT NULL THEN 'Y' ELSE '-' END AS has_ref_range,
       CASE WHEN interpretation IS NOT NULL THEN 'Y' ELSE '-' END AS has_interp,
       CASE WHEN category IS NOT NULL THEN 'Y' ELSE '-' END AS has_category,
       df_file_name
FROM {{zone_name}}.fhir.observations_clinical
ORDER BY observation_id;


-- ============================================================================
-- 5. OBSERVATION STATUS DISTRIBUTION
-- ============================================================================
-- FHIR Observation.status is a required field with values: registered |
-- preliminary | final | amended | corrected | cancelled | entered-in-error |
-- unknown. Most clinical observations are "final" after verification. This
-- query shows the status distribution across all clinical observations.

SELECT status, COUNT(*) AS obs_count
FROM {{zone_name}}.fhir.observations_clinical
GROUP BY status
ORDER BY obs_count DESC;


-- ============================================================================
-- 6. FILE PROVENANCE — Source file tracking for clinical audit
-- ============================================================================
-- Every observation can be traced back to its source FHIR JSON file.
-- In healthcare data pipelines, this lineage is essential for regulatory
-- compliance (HIPAA, GDPR) and clinical data quality auditing.

SELECT df_file_name, observation_id, status
FROM {{zone_name}}.fhir.observations_clinical
ORDER BY df_file_name;


-- ============================================================================
-- 7. PATIENT REFERENCES — Which patients have observations?
-- ============================================================================
-- FHIR Observation.subject is a Reference to the Patient resource. The bulk
-- export links observations to specific patients, enabling per-patient
-- clinical analysis. This query shows how many observations each referenced
-- patient has.

SELECT subject, COUNT(*) AS observation_count
FROM {{zone_name}}.fhir.observations_bulk
GROUP BY subject
ORDER BY observation_count DESC
LIMIT 10;


-- ============================================================================
-- 8. OBSERVATIONS PER DAY — Temporal distribution of heart rate readings
-- ============================================================================
-- Clinical observations have timestamps (effectiveDateTime). This query
-- shows the distribution of heart rate readings over time, useful for
-- identifying measurement patterns and data collection schedules.

SELECT SUBSTRING(effective_date, 1, 10) AS observation_date,
       COUNT(*) AS readings
FROM {{zone_name}}.fhir.observations_bulk
WHERE effective_date IS NOT NULL
GROUP BY SUBSTRING(effective_date, 1, 10)
ORDER BY observation_date;


-- ============================================================================
-- 9. COMBINED DATA LANDSCAPE — Total observations across both sources
-- ============================================================================
-- Overview of the complete clinical observation dataset, combining bulk
-- export data with individual clinical files.

SELECT 'Bulk NDJSON (Heart Rate)' AS source, COUNT(*) AS count
FROM {{zone_name}}.fhir.observations_bulk
UNION ALL
SELECT 'Clinical JSON (Mixed Types)' AS source, COUNT(*) AS count
FROM {{zone_name}}.fhir.observations_clinical
ORDER BY source;


-- ============================================================================
-- 10. SUMMARY — All checks in one query
-- ============================================================================

SELECT check_name, result FROM (

    -- Check 1: Bulk observation count = 100
    SELECT 'bulk_count_100' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir.observations_bulk) = 100
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Clinical observation count = 14
    SELECT 'clinical_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir.observations_clinical) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: All bulk observations have status
    SELECT 'bulk_status_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir.observations_bulk WHERE status IS NULL) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: All bulk observations have subject (patient reference)
    SELECT 'bulk_subject_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir.observations_bulk WHERE subject IS NULL) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Column mapping — observation_id exists
    SELECT 'column_mapping_obs_id' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir.observations_bulk WHERE observation_id IS NOT NULL) = 100
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: Clinical observations have code
    SELECT 'clinical_code_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir.observations_clinical WHERE code IS NOT NULL) > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: File metadata on bulk
    SELECT 'file_metadata_bulk' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir.observations_bulk WHERE df_file_name IS NOT NULL) = 100
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: Multiple source files for clinical
    SELECT 'clinical_multi_file' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.fhir.observations_clinical) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
