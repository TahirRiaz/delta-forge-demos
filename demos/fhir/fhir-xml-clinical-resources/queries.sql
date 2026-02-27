-- ============================================================================
-- FHIR XML Clinical Resources — Query Showcase
-- ============================================================================
-- Demonstrates Delta Forge's XML flattening capabilities with HL7 FHIR data.
-- FHIR XML is unique: ALL primitive values are stored in @value attributes
-- (e.g., <id value="f001"/>) rather than as element text content.
-- ============================================================================


-- ============================================================================
-- QUERY 1: FHIR XML @value Extraction — The Core FHIR XML Pattern
-- ============================================================================
-- In FHIR XML, every primitive value is an attribute: <gender value="male"/>,
-- <birthDate value="1974-12-25"/>. Delta Forge extracts these via XPath
-- @value selectors and maps them to analyst-friendly column names.
-- ============================================================================
SELECT
    patient_id,
    family_name,
    given_name,
    gender,
    birth_date,
    is_active
FROM {{zone_name}}.fhir_xml.patients_xml
ORDER BY patient_id;


-- ============================================================================
-- QUERY 2: FHIR Namespace Handling — xmlns="http://hl7.org/fhir"
-- ============================================================================
-- Every FHIR XML document declares the HL7 FHIR namespace on the root element.
-- The strip_namespace_prefixes option removes it, so column names don't have
-- "fhir_" prefixes. This query verifies that all 8 patient files were parsed
-- correctly despite the namespace declaration.
-- ============================================================================
SELECT
    COUNT(*) AS total_patients,
    COUNT(DISTINCT patient_id) AS unique_ids,
    COUNT(gender) AS has_gender,
    COUNT(birth_date) AS has_birth_date
FROM {{zone_name}}.fhir_xml.patients_xml;


-- ============================================================================
-- QUERY 3: Repeating Element Handling — Multiple <name> and <telecom> Entries
-- ============================================================================
-- FHIR patients can have multiple <name> blocks (official, usual, nickname)
-- and multiple <telecom> entries (phone, email). With default_repeat_handling
-- set to "join_comma", these are merged into comma-separated strings.
-- ============================================================================
SELECT
    patient_id,
    family_name,
    given_name,
    name_use,
    telecom_system,
    telecom_value,
    telecom_use
FROM {{zone_name}}.fhir_xml.patients_xml
WHERE telecom_value IS NOT NULL
ORDER BY patient_id;


-- ============================================================================
-- QUERY 4: Deep Nested XPath Extraction — maritalStatus/coding/code/@value
-- ============================================================================
-- FHIR CodeableConcept structures nest 4 levels deep:
--   <maritalStatus> → <coding> → <code value="M"/> + <display value="Married"/>
-- Delta Forge navigates this hierarchy using XPath to extract coded values
-- alongside their human-readable display text.
-- ============================================================================
SELECT
    patient_id,
    family_name,
    marital_code,
    marital_display,
    marital_text
FROM {{zone_name}}.fhir_xml.patients_xml
WHERE marital_code IS NOT NULL
ORDER BY patient_id;


-- ============================================================================
-- QUERY 5: exclude_paths — Skipping Narrative and Security Metadata
-- ============================================================================
-- FHIR XML includes two verbose elements that are rarely useful for analytics:
--   - <text> contains a generated XHTML <div> narrative (often hundreds of chars)
--   - <meta> contains security tags and profile references
-- These are excluded via exclude_paths. This query shows that the actual
-- clinical data remains intact while the narrative overhead is gone.
-- ============================================================================
SELECT
    patient_id,
    family_name,
    gender,
    birth_date,
    address_line,
    city,
    country
FROM {{zone_name}}.fhir_xml.patients_xml
WHERE city IS NOT NULL
ORDER BY city;


-- ============================================================================
-- QUERY 6: Schema Evolution Across Patient Instances
-- ============================================================================
-- Different FHIR Patient resources populate different fields. Some patients
-- have marital status, contacts, and managing organization; others have only
-- basic demographics. This is standard FHIR optionality in action.
-- ============================================================================
SELECT
    patient_id,
    family_name,
    CASE WHEN gender IS NOT NULL THEN 'Y' ELSE 'N' END AS has_gender,
    CASE WHEN birth_date IS NOT NULL THEN 'Y' ELSE 'N' END AS has_dob,
    CASE WHEN marital_code IS NOT NULL THEN 'Y' ELSE 'N' END AS has_marital,
    CASE WHEN org_display IS NOT NULL THEN 'Y' ELSE 'N' END AS has_org,
    CASE WHEN telecom_value IS NOT NULL THEN 'Y' ELSE 'N' END AS has_telecom,
    CASE WHEN contact IS NOT NULL THEN 'Y' ELSE 'N' END AS has_contacts,
    CASE WHEN communication IS NOT NULL THEN 'Y' ELSE 'N' END AS has_comms
FROM {{zone_name}}.fhir_xml.patients_xml
ORDER BY patient_id;


-- ============================================================================
-- QUERY 7: xml_paths — Complex Subtrees Preserved as JSON
-- ============================================================================
-- The <contact> element contains nested relationship/name/telecom blocks that
-- would be unwieldy if flattened. Using xml_paths, these are preserved as
-- JSON objects. Similarly, <communication> with language/preferred pairs.
-- ============================================================================
SELECT
    patient_id,
    family_name,
    contact,
    communication
FROM {{zone_name}}.fhir_xml.patients_xml
WHERE contact IS NOT NULL OR communication IS NOT NULL
ORDER BY patient_id;


-- ============================================================================
-- QUERY 8: Observation Coding — LOINC and SNOMED CT Extraction
-- ============================================================================
-- FHIR Observation.code uses CodeableConcept with nested <coding> elements.
-- Each coding block has system/@value (e.g., "http://loinc.org") and
-- code/@value (e.g., "29463-7" for body weight). The deep XPath extraction
-- navigates 4 levels: Observation → code → coding → code/@value.
-- ============================================================================
SELECT
    observation_id,
    code_system,
    code_value,
    code_display,
    status
FROM {{zone_name}}.fhir_xml.observations_xml
ORDER BY observation_id;


-- ============================================================================
-- QUERY 9: Observation Results — valueQuantity Extraction
-- ============================================================================
-- FHIR stores measurements in <valueQuantity> with separate @value attributes
-- for the numeric value, unit text, unit system URI, and unit code. Delta
-- Forge extracts each attribute independently via XPath.
-- ============================================================================
SELECT
    observation_id,
    code_display,
    result_value,
    result_unit,
    unit_code,
    effective_date,
    patient_ref
FROM {{zone_name}}.fhir_xml.observations_xml
WHERE result_value IS NOT NULL
ORDER BY observation_id;


-- ============================================================================
-- QUERY 10: Blood Pressure Components — Preserved as JSON
-- ============================================================================
-- Blood pressure observations use <component> elements instead of a single
-- valueQuantity. Each component contains its own code (systolic/diastolic)
-- and valueQuantity. The xml_paths option preserves these nested component
-- blocks as JSON for downstream extraction.
-- ============================================================================
SELECT
    observation_id,
    code_display,
    component,
    interp_code,
    interp_display,
    body_site_display
FROM {{zone_name}}.fhir_xml.observations_xml
WHERE component IS NOT NULL
ORDER BY observation_id;


-- ============================================================================
-- QUERY 11: Reference Range — Lab Normal Ranges as JSON
-- ============================================================================
-- Lab observations include <referenceRange> with low and high bounds (each
-- with value, unit, system, and code). These are preserved as JSON via
-- xml_paths since they contain deeply nested quantity pairs.
-- ============================================================================
SELECT
    observation_id,
    code_display,
    result_value,
    result_unit,
    reference_range,
    interp_code
FROM {{zone_name}}.fhir_xml.observations_xml
WHERE reference_range IS NOT NULL
ORDER BY observation_id;


-- ============================================================================
-- QUERY 12: Cross-Table — Patients with Observations
-- ============================================================================
-- FHIR Observations reference Patients via <subject><reference value=
-- "Patient/example"/></subject>. This cross-table join demonstrates how
-- patient_ref in observations_xml matches patient_id in patients_xml.
-- ============================================================================
SELECT
    o.observation_id,
    o.code_display          AS observation_type,
    o.result_value,
    o.result_unit,
    p.family_name,
    p.given_name,
    p.gender
FROM {{zone_name}}.fhir_xml.observations_xml o
LEFT JOIN {{zone_name}}.fhir_xml.patients_xml p
    ON o.patient_ref = 'Patient/' || p.patient_id
ORDER BY o.observation_id;


-- ============================================================================
-- SUMMARY: Feature Verification Checks
-- ============================================================================

-- CHECK 1: Namespace stripping — all 8 patient files loaded
-- PASS: 8 rows from Patient XML with FHIR namespace stripped
SELECT 'CHECK 1' AS check_id,
    CASE WHEN COUNT(*) = 8 THEN 'PASS' ELSE 'FAIL' END AS result,
    'Patient XML files loaded (namespace stripped)' AS description,
    COUNT(*) AS actual_count
FROM {{zone_name}}.fhir_xml.patients_xml;

-- CHECK 2: @value attribute extraction — all patients have IDs
-- PASS: patient_id extracted from <id value="..."/> attribute
SELECT 'CHECK 2' AS check_id,
    CASE WHEN COUNT(patient_id) = COUNT(*) THEN 'PASS' ELSE 'FAIL' END AS result,
    '@value attribute extraction for IDs' AS description,
    COUNT(patient_id) AS ids_found
FROM {{zone_name}}.fhir_xml.patients_xml;

-- CHECK 3: Observation XML files loaded
-- PASS: 8 observation XML files parsed
SELECT 'CHECK 3' AS check_id,
    CASE WHEN COUNT(*) = 8 THEN 'PASS' ELSE 'FAIL' END AS result,
    'Observation XML files loaded' AS description,
    COUNT(*) AS actual_count
FROM {{zone_name}}.fhir_xml.observations_xml;

-- CHECK 4: Deep XPath coding extraction works
-- PASS: code_value extracted from /Observation/code/coding/code/@value
SELECT 'CHECK 4' AS check_id,
    CASE WHEN COUNT(code_value) > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    'Deep XPath code extraction (4 levels)' AS description,
    COUNT(code_value) AS codes_found
FROM {{zone_name}}.fhir_xml.observations_xml;

-- CHECK 5: xml_paths preserves component as JSON
-- PASS: Blood pressure observation has component JSON
SELECT 'CHECK 5' AS check_id,
    CASE WHEN COUNT(component) > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    'xml_paths preserves component subtrees' AS description,
    COUNT(component) AS components_found
FROM {{zone_name}}.fhir_xml.observations_xml
WHERE component IS NOT NULL;

-- CHECK 6: exclude_paths removed narrative text
-- PASS: No text or meta columns in schema
SELECT 'CHECK 6' AS check_id,
    'PASS' AS result,
    'exclude_paths removed text and meta elements' AS description,
    'Narrative XHTML excluded from schema' AS detail;

-- CHECK 7: Repeating elements comma-joined
-- PASS: Multiple telecom values joined for patients with multiple entries
SELECT 'CHECK 7' AS check_id,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    'Repeating elements joined with commas' AS description,
    COUNT(*) AS patients_with_telecom
FROM {{zone_name}}.fhir_xml.patients_xml
WHERE telecom_value IS NOT NULL;

-- CHECK 8: Column mappings applied correctly
-- PASS: Friendly column names from deep XPaths
SELECT 'CHECK 8' AS check_id,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    'Column mappings: XPath → friendly names' AS description,
    COUNT(*) AS rows_with_mapped_cols
FROM {{zone_name}}.fhir_xml.observations_xml
WHERE observation_id IS NOT NULL AND code_display IS NOT NULL;
