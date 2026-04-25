-- ============================================================================
-- Multi-Vendor Marketplace — Multiple Indexes on the Same Table
-- ============================================================================
-- WHAT: A table can carry several row-level indexes side by side.
-- WHY:  Different query shapes benefit from differently-keyed
--       indexes. The planner picks the cheapest applicable index per
--       query, falling back to ordinary file pruning when no index
--       beats it.
-- HOW:  This demo has three indexes — sku, brand, (category, price).
--       The queries below each match one of those (or none, to show
--       graceful fallback).
-- ============================================================================


-- ============================================================================
-- BUILD: Create Three Indexes for Three Search Shapes
-- ============================================================================
-- Each search shape on the marketplace gets its own index. The
-- planner picks the cheapest applicable one per query at runtime —
-- the caller writes ordinary SQL.
--
-- 1. idx_sku            : warehouse fulfillment, exact SKU lookup
-- 2. idx_brand          : storefront landing pages, equality on brand
-- 3. idx_category_price : faceted browse, leading-column equality
--                         plus a trailing range on price

CREATE INDEX idx_sku
    ON TABLE {{zone_name}}.delta_demos.marketplace_listings (sku)
    WITH (auto_update = true);

CREATE INDEX idx_brand
    ON TABLE {{zone_name}}.delta_demos.marketplace_listings (brand)
    WITH (auto_update = true);

CREATE INDEX idx_category_price
    ON TABLE {{zone_name}}.delta_demos.marketplace_listings (category, price)
    WITH (auto_update = true);


-- ============================================================================
-- EXPLORE: Inventory Mix
-- ============================================================================
-- 70 listings across 6 brands and 5 categories.

ASSERT ROW_COUNT = 6
ASSERT VALUE listing_count = 12 WHERE brand = 'AcmeAudio'
ASSERT VALUE listing_count = 12 WHERE brand = 'Bellweather'
ASSERT VALUE listing_count = 12 WHERE brand = 'Crestwood'
ASSERT VALUE listing_count = 12 WHERE brand = 'Driftvale'
ASSERT VALUE listing_count = 11 WHERE brand = 'Emberforge'
ASSERT VALUE listing_count = 11 WHERE brand = 'Foxkin'
SELECT brand,
       COUNT(*)         AS listing_count,
       SUM(stock)       AS total_stock,
       ROUND(AVG(price), 2) AS avg_price
FROM {{zone_name}}.delta_demos.marketplace_listings
GROUP BY brand
ORDER BY brand;


-- ============================================================================
-- LEARN: Warehouse SKU Lookup — `idx_sku` Wins
-- ============================================================================
-- Pulling SKU-EM-5002 for fulfillment. The selector picks idx_sku
-- because it offers the most narrowing for an equality predicate on
-- sku. The other two indexes are not applicable to this predicate.

ASSERT ROW_COUNT = 1
ASSERT VALUE sku = 'SKU-EM-5002'
ASSERT VALUE title = 'Enameled Dutch Oven'
ASSERT VALUE brand = 'Emberforge'
ASSERT VALUE price = 149.0
ASSERT VALUE stock = 41
SELECT sku, title, brand, category, price, stock
FROM {{zone_name}}.delta_demos.marketplace_listings
WHERE sku = 'SKU-EM-5002';


-- ============================================================================
-- LEARN: Storefront Brand Filter — `idx_brand` Wins
-- ============================================================================
-- Storefront landing page for Crestwood. The brand index narrows to
-- exactly the slices carrying Crestwood rows.

ASSERT ROW_COUNT = 1
ASSERT VALUE listing_count = 12
ASSERT VALUE total_stock = 1142
-- Non-deterministic: AVG of DOUBLE column — minor float variance possible across platforms
ASSERT WARNING VALUE avg_price BETWEEN 85.91 AND 85.93
SELECT COUNT(*)              AS listing_count,
       SUM(stock)            AS total_stock,
       ROUND(AVG(price), 2)  AS avg_price
FROM {{zone_name}}.delta_demos.marketplace_listings
WHERE brand = 'Crestwood';


-- ============================================================================
-- LEARN: Faceted Browse — `idx_category_price` Wins
-- ============================================================================
-- Browse page: outdoor gear between $50 and $150. The composite
-- index uses both columns: leading category narrows the slices, then
-- the trailing price range narrows further within each.

ASSERT ROW_COUNT = 1
ASSERT VALUE listing_count = 5
ASSERT VALUE max_price = 139.0
ASSERT VALUE total_stock = 311
SELECT COUNT(*)              AS listing_count,
       MAX(price)            AS max_price,
       SUM(stock)            AS total_stock
FROM {{zone_name}}.delta_demos.marketplace_listings
WHERE category = 'outdoor'
  AND price BETWEEN 50 AND 150;


-- ============================================================================
-- LEARN: Leftmost-Prefix on the Composite — Still Helps
-- ============================================================================
-- A predicate on category alone uses idx_category_price's leading
-- column. The selector picks it; the trailing price column simply
-- isn't constrained.

ASSERT ROW_COUNT = 1
ASSERT VALUE listing_count = 23
ASSERT VALUE total_stock = 2022
SELECT COUNT(*)        AS listing_count,
       SUM(stock)      AS total_stock
FROM {{zone_name}}.delta_demos.marketplace_listings
WHERE category = 'apparel';


-- ============================================================================
-- LEARN: No Index Applies — Graceful Fallback
-- ============================================================================
-- Looking for low-stock alerts: predicate is on stock alone. None of
-- the three indexes is keyed on stock, so the selector falls back to
-- ordinary file pruning. The query still runs correctly — the index
-- subsystem simply does nothing.

ASSERT ROW_COUNT = 12
ASSERT VALUE stock < 20
SELECT listing_id, brand, category, title, stock
FROM {{zone_name}}.delta_demos.marketplace_listings
WHERE stock < 20
ORDER BY stock, listing_id;


-- ============================================================================
-- LEARN: SHOW INDEXES — Inventory of Available Indexes
-- ============================================================================
-- Operators inspect what's available. This is what the selector
-- consults internally for every query.

SHOW INDEXES ON TABLE {{zone_name}}.delta_demos.marketplace_listings;


-- ============================================================================
-- LEARN: DROP a Redundant Index
-- ============================================================================
-- Auditing reveals brand searches are rare; the index isn't worth
-- its storage and write cost. DROP removes it; future queries that
-- would have used it fall back to the next-best applicable index
-- (or to file pruning if none applies).

DROP INDEX IF EXISTS idx_brand
    ON TABLE {{zone_name}}.delta_demos.marketplace_listings;

SHOW INDEXES ON TABLE {{zone_name}}.delta_demos.marketplace_listings;


-- ============================================================================
-- LEARN: Brand Query After DROP — Same Answer, Different Path
-- ============================================================================
-- The same brand filter still works. Without idx_brand the selector
-- falls back to file pruning. Result is identical.

ASSERT ROW_COUNT = 1
ASSERT VALUE listing_count = 12
ASSERT VALUE total_stock = 1142
SELECT COUNT(*)              AS listing_count,
       SUM(stock)            AS total_stock
FROM {{zone_name}}.delta_demos.marketplace_listings
WHERE brand = 'Crestwood';


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_listings = 70
ASSERT VALUE total_stock = 5284
ASSERT VALUE distinct_brands = 6
ASSERT VALUE distinct_categories = 5
ASSERT VALUE distinct_sellers = 6
-- Non-deterministic: SUM of DOUBLE column — minor float variance possible across platforms
ASSERT WARNING VALUE total_price BETWEEN 8454.4 AND 8454.6
SELECT COUNT(*)                              AS total_listings,
       SUM(stock)                            AS total_stock,
       COUNT(DISTINCT brand)                 AS distinct_brands,
       COUNT(DISTINCT category)              AS distinct_categories,
       COUNT(DISTINCT seller_id)             AS distinct_sellers,
       ROUND(SUM(price), 2)                  AS total_price
FROM {{zone_name}}.delta_demos.marketplace_listings;
