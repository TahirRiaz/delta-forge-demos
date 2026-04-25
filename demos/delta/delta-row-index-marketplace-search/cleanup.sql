-- Cleanup: Multi-Vendor Marketplace — Multiple Indexes on the Same Table

DROP INDEX IF EXISTS idx_sku            ON TABLE {{zone_name}}.delta_demos.marketplace_listings;
DROP INDEX IF EXISTS idx_brand          ON TABLE {{zone_name}}.delta_demos.marketplace_listings;
DROP INDEX IF EXISTS idx_category_price ON TABLE {{zone_name}}.delta_demos.marketplace_listings;

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.marketplace_listings WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
