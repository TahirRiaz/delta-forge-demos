-- ============================================================================
-- FHIR XML Clinical Resources — Cleanup Script
-- ============================================================================
-- Removes the tables and schema created by this demo.
-- ============================================================================

DROP TABLE IF EXISTS {{zone_name}}.fhir_xml.patients_xml;
DROP TABLE IF EXISTS {{zone_name}}.fhir_xml.observations_xml;
DROP SCHEMA IF EXISTS {{zone_name}}.fhir_xml;
