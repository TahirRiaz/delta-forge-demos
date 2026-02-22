-- ============================================================================
-- XML Books Schema Evolution — Setup Script
-- ============================================================================
-- Creates one external table that reads 5 XML files spanning 2000–2004.
-- Each file adds or removes elements, demonstrating schema evolution.
--
-- The xml_flatten_config pre-selects the union of all paths across all files.
-- DETECT SCHEMA then uses this config to generate the column definitions
-- (ConfigBasedStrategy), so no file I/O is needed at schema-discovery time.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.xml
    COMMENT 'XML-backed external tables';

-- ============================================================================
-- TABLE: books_evolved — All 5 catalog files (schema evolution)
-- ============================================================================
-- The xml_flatten_config specifies:
--   row_xpath        — //book  (each <book> element becomes a row)
--   include_paths    — union of all leaf elements + attributes across 5 files
--   include_attributes — true (extract @id and @format as columns)
--   separator        — _ (nested paths join with underscore)
--
-- Column naming convention (from XmlFlattenConfig.column_name):
--   /catalog/book/@id    →  attr_id      (@ → attr_)
--   /catalog/book/author →  author       (leaf element, no prefix)
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.xml.books_evolved
USING XML
LOCATION '{{data_path}}'
OPTIONS (
    xml_flatten_config = '{
        "row_xpath": "//book",
        "include_paths": [
            "/catalog/book/@id",
            "/catalog/book/@format",
            "/catalog/book/author",
            "/catalog/book/title",
            "/catalog/book/genre",
            "/catalog/book/price",
            "/catalog/book/publish_date",
            "/catalog/book/description",
            "/catalog/book/isbn",
            "/catalog/book/language",
            "/catalog/book/publisher",
            "/catalog/book/rating",
            "/catalog/book/edition",
            "/catalog/book/pages",
            "/catalog/book/series"
        ],
        "include_attributes": true,
        "separator": "_",
        "max_depth": 10,
        "strip_namespace_prefixes": true
    }',
    file_metadata = '{"columns":["df_file_name","df_file_modified","df_dataset","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.xml.books_evolved;
GRANT READ ON TABLE {{zone_name}}.xml.books_evolved TO USER {{current_user}};
