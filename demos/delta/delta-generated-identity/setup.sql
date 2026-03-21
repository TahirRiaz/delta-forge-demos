-- ============================================================================
-- Delta Generated & Identity Columns — Setup Script
-- ============================================================================
-- Creates tables for the computed-column and sequential-ID demo.
--
-- Tables created:
--   1. order_items     — Order line items (populated in queries.sql)
--   2. event_sequence  — Sequential event tracking (populated in queries.sql)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: order_items — Order line items with computed financial columns
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.order_items (
    id          INT,
    product     VARCHAR,
    qty         INT,
    unit_price  DOUBLE,
    subtotal    DOUBLE,
    tax         DOUBLE,
    total       DOUBLE,
    order_date  VARCHAR
) LOCATION '{{data_path}}/order_items';

DETECT SCHEMA FOR TABLE {{zone_name}}.delta_demos.order_items;
GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.order_items TO USER {{current_user}};


-- ============================================================================
-- TABLE: event_sequence — Sequential event tracking
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.event_sequence (
    seq_id      BIGINT,
    event_type  VARCHAR,
    payload     VARCHAR,
    created_at  VARCHAR
) LOCATION '{{data_path}}/event_sequence';

DETECT SCHEMA FOR TABLE {{zone_name}}.delta_demos.event_sequence;
GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.event_sequence TO USER {{current_user}};
