-- Cleanup: E-Commerce Order Tracking — Indexed UPDATE / DELETE / MERGE

DROP INDEX IF EXISTS idx_tracking ON TABLE {{zone_name}}.delta_demos.shipment_orders;

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.shipment_orders WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
