-- ============================================================================
-- XML Books Schema Evolution — Setup Script
-- ============================================================================
-- Creates one external table that reads 5 XML files spanning 2000–2004.
-- Each file adds or removes elements, demonstrating schema evolution.
-- DETECT SCHEMA discovers the union of all XML paths and stores the
-- xml_flatten_config in the catalog so it can be retrieved on every query.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS external TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS external.xml
    COMMENT 'XML-backed external tables';

-- ============================================================================
-- TABLE: books_evolved — All 5 catalog files (schema evolution)
-- ============================================================================
-- Reads every .xml file in the data directory. The 5 files have different
-- element sets; DETECT SCHEMA finds the union and saves the flatten config.
CREATE EXTERNAL TABLE IF NOT EXISTS external.xml.books_evolved
USING XML
LOCATION '{{data_path}}'
OPTIONS (
    has_header = 'true'
);
DETECT SCHEMA FOR TABLE external.xml.books_evolved;
GRANT READ ON TABLE external.xml.books_evolved TO USER {{current_user}};
