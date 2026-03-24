-- ============================================================================
-- Delta Z-ORDER & Transaction Management — Setup Script
-- ============================================================================
-- Creates the web_analytics table and loads 100 events across 3 batch inserts
-- to simulate fragmented data files — the scenario that OPTIMIZE ZORDER fixes.
--
-- Tables created:
--   1. web_analytics — 100 web events across 3 batches
--
-- Operations performed:
--   1. Zone & schema creation
--   2. CREATE DELTA TABLE
--   3. INSERT batch 1 — 40 page view events
--   4. INSERT batch 2 — 30 click events
--   5. INSERT batch 3 — 30 conversion events
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: web_analytics — web events with multi-dimensional access patterns
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.web_analytics (
    id              INT,
    session_id      VARCHAR,
    user_id         VARCHAR,
    page_url        VARCHAR,
    event_type      VARCHAR,
    duration_ms     INT,
    browser         VARCHAR,
    country         VARCHAR,
    event_date      VARCHAR
) LOCATION '{{data_path}}/web_analytics';

GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.web_analytics TO USER {{current_user}};


-- ============================================================================
-- STEP 2: Batch 1 — 40 page view events
-- ============================================================================
-- Each batch INSERT creates separate data files on disk. Three batches means
-- at least three files, which is the fragmentation that OPTIMIZE will fix.
INSERT INTO {{zone_name}}.delta_demos.web_analytics VALUES
    (1,  'S001', 'U001', '/home',         'pageview', 3200, 'Chrome',  'US', '2025-01-01'),
    (2,  'S002', 'U002', '/products',     'pageview', 5100, 'Firefox', 'UK', '2025-01-01'),
    (3,  'S003', 'U003', '/about',        'pageview', 1800, 'Safari',  'DE', '2025-01-01'),
    (4,  'S004', 'U004', '/pricing',      'pageview', 4500, 'Chrome',  'FR', '2025-01-01'),
    (5,  'S005', 'U005', '/blog',         'pageview', 7200, 'Edge',    'US', '2025-01-01'),
    (6,  'S006', 'U006', '/home',         'pageview', 2100, 'Chrome',  'JP', '2025-01-02'),
    (7,  'S007', 'U007', '/products',     'pageview', 6300, 'Firefox', 'US', '2025-01-02'),
    (8,  'S008', 'U008', '/docs',         'pageview', 9800, 'Chrome',  'IN', '2025-01-02'),
    (9,  'S009', 'U009', '/blog/post-1',  'pageview', 4200, 'Safari',  'BR', '2025-01-02'),
    (10, 'S010', 'U010', '/pricing',      'pageview', 3600, 'Chrome',  'US', '2025-01-02'),
    (11, 'S011', 'U011', '/home',         'pageview', 500,  'Chrome',  'CA', '2025-01-03'),
    (12, 'S012', 'U012', '/products',     'pageview', 4100, 'Firefox', 'AU', '2025-01-03'),
    (13, 'S013', 'U013', '/about',        'pageview', 2800, 'Safari',  'US', '2025-01-03'),
    (14, 'S014', 'U014', '/blog/post-2',  'pageview', 6700, 'Chrome',  'UK', '2025-01-03'),
    (15, 'S015', 'U015', '/docs/api',     'pageview', 8500, 'Edge',    'DE', '2025-01-03'),
    (16, 'S016', 'U016', '/home',         'pageview', 300,  'Chrome',  'US', '2025-01-04'),
    (17, 'S017', 'U017', '/products',     'pageview', 5500, 'Firefox', 'FR', '2025-01-04'),
    (18, 'S018', 'U018', '/pricing',      'pageview', 4800, 'Chrome',  'JP', '2025-01-04'),
    (19, 'S019', 'U019', '/blog/post-3',  'pageview', 7100, 'Safari',  'US', '2025-01-04'),
    (20, 'S020', 'U020', '/docs/guides',  'pageview', 11200,'Chrome',  'IN', '2025-01-04'),
    (21, 'S021', 'U021', '/home',         'pageview', 2500, 'Chrome',  'MX', '2025-01-05'),
    (22, 'S022', 'U022', '/products',     'pageview', 3900, 'Firefox', 'US', '2025-01-05'),
    (23, 'S023', 'U023', '/about',        'pageview', 1500, 'Safari',  'ES', '2025-01-05'),
    (24, 'S024', 'U024', '/pricing',      'pageview', 5200, 'Chrome',  'US', '2025-01-05'),
    (25, 'S025', 'U025', '/blog',         'pageview', 800,  'Edge',    'IT', '2025-01-05'),
    (26, 'S026', 'U026', '/home',         'pageview', 4000, 'Chrome',  'US', '2025-01-06'),
    (27, 'S027', 'U027', '/products',     'pageview', 6100, 'Firefox', 'KR', '2025-01-06'),
    (28, 'S028', 'U028', '/docs',         'pageview', 10500,'Chrome',  'US', '2025-01-06'),
    (29, 'S029', 'U029', '/blog/post-4',  'pageview', 3300, 'Safari',  'NL', '2025-01-06'),
    (30, 'S030', 'U030', '/pricing',      'pageview', 4700, 'Chrome',  'SE', '2025-01-06'),
    (31, 'S031', 'U031', '/home',         'pageview', 200,  'Chrome',  'US', '2025-01-07'),
    (32, 'S032', 'U032', '/products',     'pageview', 5800, 'Firefox', 'UK', '2025-01-07'),
    (33, 'S033', 'U033', '/about',        'pageview', 2200, 'Safari',  'US', '2025-01-07'),
    (34, 'S034', 'U034', '/blog/post-5',  'pageview', 7500, 'Chrome',  'DE', '2025-01-07'),
    (35, 'S035', 'U035', '/docs/api',     'pageview', 9200, 'Edge',    'US', '2025-01-07'),
    (36, 'S036', 'U036', '/home',         'pageview', 3100, 'Chrome',  'FR', '2025-01-08'),
    (37, 'S037', 'U037', '/products',     'pageview', 4400, 'Firefox', 'US', '2025-01-08'),
    (38, 'S038', 'U038', '/pricing',      'pageview', 5900, 'Chrome',  'BR', '2025-01-08'),
    (39, 'S039', 'U039', '/blog',         'pageview', 6800, 'Safari',  'US', '2025-01-08'),
    (40, 'S040', 'U040', '/docs/guides',  'pageview', 12000,'Chrome',  'JP', '2025-01-08');


-- ============================================================================
-- STEP 3: Batch 2 — 30 click events
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.web_analytics
SELECT * FROM (VALUES
    (41, 'S001', 'U001', '/products/laptop',    'click', 150,  'Chrome',  'US', '2025-01-01'),
    (42, 'S002', 'U002', '/products/phone',     'click', 200,  'Firefox', 'UK', '2025-01-01'),
    (43, 'S004', 'U004', '/pricing/enterprise', 'click', 180,  'Chrome',  'FR', '2025-01-01'),
    (44, 'S007', 'U007', '/products/tablet',    'click', 120,  'Firefox', 'US', '2025-01-02'),
    (45, 'S008', 'U008', '/docs/api/auth',      'click', 90,   'Chrome',  'IN', '2025-01-02'),
    (46, 'S010', 'U010', '/pricing/team',       'click', 250,  'Chrome',  'US', '2025-01-02'),
    (47, 'S012', 'U012', '/products/monitor',   'click', 170,  'Firefox', 'AU', '2025-01-03'),
    (48, 'S014', 'U014', '/blog/subscribe',     'click', 80,   'Chrome',  'UK', '2025-01-03'),
    (49, 'S017', 'U017', '/products/keyboard',  'click', 140,  'Firefox', 'FR', '2025-01-04'),
    (50, 'S018', 'U018', '/pricing/individual', 'click', 300,  'Chrome',  'JP', '2025-01-04'),
    (51, 'S020', 'U020', '/docs/guides/start',  'click', 110,  'Chrome',  'IN', '2025-01-04'),
    (52, 'S022', 'U022', '/products/headphones','click', 160,  'Firefox', 'US', '2025-01-05'),
    (53, 'S024', 'U024', '/pricing/enterprise', 'click', 220,  'Chrome',  'US', '2025-01-05'),
    (54, 'S027', 'U027', '/products/speaker',   'click', 130,  'Firefox', 'KR', '2025-01-06'),
    (55, 'S028', 'U028', '/docs/faq',           'click', 100,  'Chrome',  'US', '2025-01-06'),
    (56, 'S032', 'U032', '/products/webcam',    'click', 190,  'Firefox', 'UK', '2025-01-07'),
    (57, 'S034', 'U034', '/blog/comment',       'click', 70,   'Chrome',  'DE', '2025-01-07'),
    (58, 'S037', 'U037', '/products/mouse',     'click', 145,  'Firefox', 'US', '2025-01-08'),
    (59, 'S038', 'U038', '/pricing/compare',    'click', 280,  'Chrome',  'BR', '2025-01-08'),
    (60, 'S040', 'U040', '/docs/changelog',     'click', 95,   'Chrome',  'JP', '2025-01-08'),
    (61, 'S001', 'U001', '/products/laptop/buy','click', 320,  'Chrome',  'US', '2025-01-01'),
    (62, 'S007', 'U007', '/products/tablet/buy','click', 280,  'Firefox', 'US', '2025-01-02'),
    (63, 'S010', 'U010', '/pricing/checkout',   'click', 400,  'Chrome',  'US', '2025-01-02'),
    (64, 'S017', 'U017', '/products/keyboard/buy','click',200, 'Firefox', 'FR', '2025-01-04'),
    (65, 'S022', 'U022', '/products/headphones/buy','click',180,'Firefox','US', '2025-01-05'),
    (66, 'S024', 'U024', '/pricing/checkout',   'click', 350,  'Chrome',  'US', '2025-01-05'),
    (67, 'S032', 'U032', '/products/webcam/buy','click', 210,  'Firefox', 'UK', '2025-01-07'),
    (68, 'S037', 'U037', '/products/mouse/buy', 'click', 175,  'Firefox', 'US', '2025-01-08'),
    (69, 'S038', 'U038', '/pricing/checkout',   'click', 420,  'Chrome',  'BR', '2025-01-08'),
    (70, 'S004', 'U004', '/pricing/checkout',   'click', 380,  'Chrome',  'FR', '2025-01-01')
) AS t(id, session_id, user_id, page_url, event_type, duration_ms, browser, country, event_date);


-- ============================================================================
-- STEP 4: Batch 3 — 30 conversion events
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.web_analytics
SELECT * FROM (VALUES
    (71,  'S001', 'U001', '/checkout/complete',  'conversion', 450,  'Chrome',  'US', '2025-01-01'),
    (72,  'S004', 'U004', '/checkout/complete',  'conversion', 520,  'Chrome',  'FR', '2025-01-01'),
    (73,  'S007', 'U007', '/checkout/complete',  'conversion', 380,  'Firefox', 'US', '2025-01-02'),
    (74,  'S010', 'U010', '/checkout/complete',  'conversion', 490,  'Chrome',  'US', '2025-01-02'),
    (75,  'S014', 'U014', '/newsletter/confirm', 'conversion', 60,   'Chrome',  'UK', '2025-01-03'),
    (76,  'S017', 'U017', '/checkout/complete',  'conversion', 410,  'Firefox', 'FR', '2025-01-04'),
    (77,  'S020', 'U020', '/signup/complete',    'conversion', 300,  'Chrome',  'IN', '2025-01-04'),
    (78,  'S022', 'U022', '/checkout/complete',  'conversion', 470,  'Firefox', 'US', '2025-01-05'),
    (79,  'S024', 'U024', '/checkout/complete',  'conversion', 550,  'Chrome',  'US', '2025-01-05'),
    (80,  'S027', 'U027', '/signup/complete',    'conversion', 280,  'Firefox', 'KR', '2025-01-06'),
    (81,  'S032', 'U032', '/checkout/complete',  'conversion', 430,  'Firefox', 'UK', '2025-01-07'),
    (82,  'S034', 'U034', '/newsletter/confirm', 'conversion', 50,   'Chrome',  'DE', '2025-01-07'),
    (83,  'S037', 'U037', '/checkout/complete',  'conversion', 390,  'Firefox', 'US', '2025-01-08'),
    (84,  'S038', 'U038', '/checkout/complete',  'conversion', 510,  'Chrome',  'BR', '2025-01-08'),
    (85,  'S040', 'U040', '/signup/complete',    'conversion', 260,  'Chrome',  'JP', '2025-01-08'),
    (86,  'S002', 'U002', '/checkout/complete',  'conversion', 440,  'Firefox', 'UK', '2025-01-01'),
    (87,  'S005', 'U005', '/newsletter/confirm', 'conversion', 70,   'Edge',    'US', '2025-01-01'),
    (88,  'S009', 'U009', '/signup/complete',    'conversion', 310,  'Safari',  'BR', '2025-01-02'),
    (89,  'S012', 'U012', '/checkout/complete',  'conversion', 460,  'Firefox', 'AU', '2025-01-03'),
    (90,  'S015', 'U015', '/signup/complete',    'conversion', 290,  'Edge',    'DE', '2025-01-03'),
    (91,  'S018', 'U018', '/checkout/complete',  'conversion', 500,  'Chrome',  'JP', '2025-01-04'),
    (92,  'S021', 'U021', '/signup/complete',    'conversion', 270,  'Chrome',  'MX', '2025-01-05'),
    (93,  'S026', 'U026', '/newsletter/confirm', 'conversion', 55,   'Chrome',  'US', '2025-01-06'),
    (94,  'S029', 'U029', '/signup/complete',    'conversion', 320,  'Safari',  'NL', '2025-01-06'),
    (95,  'S030', 'U030', '/newsletter/confirm', 'conversion', 45,   'Chrome',  'SE', '2025-01-06'),
    (96,  'S033', 'U033', '/signup/complete',    'conversion', 340,  'Safari',  'US', '2025-01-07'),
    (97,  'S035', 'U035', '/signup/complete',    'conversion', 305,  'Edge',    'US', '2025-01-07'),
    (98,  'S036', 'U036', '/checkout/complete',  'conversion', 480,  'Chrome',  'FR', '2025-01-08'),
    (99,  'S039', 'U039', '/newsletter/confirm', 'conversion', 65,   'Safari',  'US', '2025-01-08'),
    (100, 'S006', 'U006', '/signup/complete',    'conversion', 350,  'Chrome',  'JP', '2025-01-02')
) AS t(id, session_id, user_id, page_url, event_type, duration_ms, browser, country, event_date);
