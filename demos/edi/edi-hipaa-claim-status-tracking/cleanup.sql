-- ============================================================================
-- EDI HIPAA Claim Status Tracking — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop External Tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.edi_demos.status_messages WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.edi_demos.status_details WITH FILES;

-- STEP 2: Drop Schema
DROP SCHEMA IF EXISTS {{zone_name}}.edi_demos;

-- STEP 3: Drop Zone
DROP ZONE IF EXISTS {{zone_name}};
