-- ============================================================================
-- Delta Variant Data Type — Semi-Structured Data — Setup Script
-- ============================================================================
-- Demonstrates semi-structured data patterns in Delta tables:
--   - VARCHAR columns storing JSON-like payload strings
--   - Different event types with varying payload structures
--   - Flexible schema within fixed Delta table columns
--
-- Tables created:
--   1. api_events — 50 API gateway events with semi-structured payloads
--
-- Operations performed:
--   1. CREATE DELTA TABLE with 7 columns
--   2. INSERT INTO VALUES — 20 login events
--   3. INSERT — 15 purchase events
--   4. INSERT — 10 error events
--   5. INSERT — 5 system events
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: api_events — 50 API gateway events with JSON-like payloads
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.api_events (
    id             INT,
    event_type     VARCHAR,
    source         VARCHAR,
    payload        VARCHAR,
    metadata       VARCHAR,
    severity       VARCHAR,
    created_at     VARCHAR
) LOCATION '{{data_path}}/api_events';

GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.api_events TO USER {{current_user}};

-- STEP 2: Insert 20 login events
INSERT INTO {{zone_name}}.delta_demos.api_events VALUES
    (1,  'login', 'web-app',     '{"user":"alice","ip":"10.0.0.1","method":"oauth"}',        '{"browser":"chrome","os":"windows"}',  'info',    '2024-01-15 08:30:00'),
    (2,  'login', 'web-app',     '{"user":"bob","ip":"10.0.0.2","method":"password"}',       '{"browser":"firefox","os":"macos"}',   'info',    '2024-01-15 08:31:00'),
    (3,  'login', 'mobile-app',  '{"user":"carol","ip":"10.0.1.1","method":"biometric"}',    '{"device":"iphone","os":"ios"}',       'info',    '2024-01-15 08:32:00'),
    (4,  'login', 'web-app',     '{"user":"dave","ip":"10.0.0.3","method":"oauth"}',         '{"browser":"safari","os":"macos"}',    'info',    '2024-01-15 08:33:00'),
    (5,  'login', 'mobile-app',  '{"user":"eve","ip":"10.0.1.2","method":"password"}',       '{"device":"android","os":"android"}',  'info',    '2024-01-15 08:34:00'),
    (6,  'login', 'web-app',     '{"user":"frank","ip":"10.0.0.4","method":"sso"}',          '{"browser":"chrome","os":"linux"}',    'info',    '2024-01-15 08:35:00'),
    (7,  'login', 'api-gateway', '{"user":"grace","ip":"10.0.2.1","method":"api-key"}',      '{"client":"postman","version":"10"}',  'info',    '2024-01-15 08:36:00'),
    (8,  'login', 'web-app',     '{"user":"henry","ip":"10.0.0.5","method":"oauth"}',        '{"browser":"edge","os":"windows"}',    'info',    '2024-01-15 08:37:00'),
    (9,  'login', 'mobile-app',  '{"user":"irene","ip":"10.0.1.3","method":"biometric"}',    '{"device":"ipad","os":"ios"}',         'info',    '2024-01-15 08:38:00'),
    (10, 'login', 'web-app',     '{"user":"jack","ip":"10.0.0.6","method":"password"}',      '{"browser":"chrome","os":"windows"}',  'info',    '2024-01-15 08:39:00'),
    (11, 'login', 'api-gateway', '{"user":"karen","ip":"10.0.2.2","method":"api-key"}',      '{"client":"curl","version":"7.88"}',   'info',    '2024-01-15 08:40:00'),
    (12, 'login', 'web-app',     '{"user":"leo","ip":"10.0.0.7","method":"sso"}',            '{"browser":"firefox","os":"linux"}',   'info',    '2024-01-15 08:41:00'),
    (13, 'login', 'mobile-app',  '{"user":"maria","ip":"10.0.1.4","method":"oauth"}',        '{"device":"pixel","os":"android"}',    'info',    '2024-01-15 08:42:00'),
    (14, 'login', 'web-app',     '{"user":"nick","ip":"10.0.0.8","method":"password"}',      '{"browser":"chrome","os":"macos"}',    'info',    '2024-01-15 08:43:00'),
    (15, 'login', 'internal',    '{"user":"olivia","ip":"192.168.1.1","method":"ldap"}',     '{"client":"admin-panel","version":"3"}','info',   '2024-01-15 08:44:00'),
    (16, 'login', 'web-app',     '{"user":"peter","ip":"10.0.0.9","method":"oauth"}',        '{"browser":"safari","os":"ios"}',      'info',    '2024-01-15 08:45:00'),
    (17, 'login', 'mobile-app',  '{"user":"quinn","ip":"10.0.1.5","method":"biometric"}',    '{"device":"samsung","os":"android"}',  'info',    '2024-01-15 08:46:00'),
    (18, 'login', 'api-gateway', '{"user":"rachel","ip":"10.0.2.3","method":"api-key"}',     '{"client":"insomnia","version":"8"}',  'info',    '2024-01-15 08:47:00'),
    (19, 'login', 'web-app',     '{"user":"sam","ip":"10.0.0.10","method":"sso"}',           '{"browser":"edge","os":"windows"}',    'info',    '2024-01-15 08:48:00'),
    (20, 'login', 'internal',    '{"user":"tina","ip":"192.168.1.2","method":"ldap"}',       '{"client":"admin-panel","version":"3"}','info',   '2024-01-15 08:49:00');


-- ============================================================================
-- STEP 3: Insert 15 purchase events
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.api_events
SELECT * FROM (VALUES
    (21, 'purchase', 'web-app',     '{"item":"laptop","amount":999.99,"currency":"USD"}',       '{"cart_id":"C100","session":"S200"}',  'info',    '2024-01-15 09:00:00'),
    (22, 'purchase', 'web-app',     '{"item":"mouse","amount":29.99,"currency":"USD"}',         '{"cart_id":"C101","session":"S201"}',  'info',    '2024-01-15 09:01:00'),
    (23, 'purchase', 'mobile-app',  '{"item":"keyboard","amount":79.99,"currency":"USD"}',      '{"cart_id":"C102","session":"S202"}',  'info',    '2024-01-15 09:02:00'),
    (24, 'purchase', 'web-app',     '{"item":"monitor","amount":449.99,"currency":"EUR"}',      '{"cart_id":"C103","session":"S203"}',  'info',    '2024-01-15 09:03:00'),
    (25, 'purchase', 'mobile-app',  '{"item":"headphones","amount":199.99,"currency":"USD"}',   '{"cart_id":"C104","session":"S204"}',  'info',    '2024-01-15 09:04:00'),
    (26, 'purchase', 'web-app',     '{"item":"webcam","amount":89.99,"currency":"USD"}',        '{"cart_id":"C105","session":"S205"}',  'info',    '2024-01-15 09:05:00'),
    (27, 'purchase', 'api-gateway', '{"item":"api-credits","amount":500.00,"currency":"USD"}',  '{"cart_id":"C106","session":"S206"}',  'info',    '2024-01-15 09:06:00'),
    (28, 'purchase', 'web-app',     '{"item":"ssd-drive","amount":129.99,"currency":"USD"}',    '{"cart_id":"C107","session":"S207"}',  'info',    '2024-01-15 09:07:00'),
    (29, 'purchase', 'mobile-app',  '{"item":"tablet","amount":599.99,"currency":"GBP"}',       '{"cart_id":"C108","session":"S208"}',  'info',    '2024-01-15 09:08:00'),
    (30, 'purchase', 'web-app',     '{"item":"charger","amount":39.99,"currency":"USD"}',       '{"cart_id":"C109","session":"S209"}',  'info',    '2024-01-15 09:09:00'),
    (31, 'purchase', 'web-app',     '{"item":"desk-lamp","amount":54.99,"currency":"USD"}',     '{"cart_id":"C110","session":"S210"}',  'info',    '2024-01-15 09:10:00'),
    (32, 'purchase', 'mobile-app',  '{"item":"phone-case","amount":24.99,"currency":"USD"}',    '{"cart_id":"C111","session":"S211"}',  'info',    '2024-01-15 09:11:00'),
    (33, 'purchase', 'web-app',     '{"item":"usb-hub","amount":44.99,"currency":"EUR"}',       '{"cart_id":"C112","session":"S212"}',  'info',    '2024-01-15 09:12:00'),
    (34, 'purchase', 'internal',    '{"item":"server-rack","amount":2499.99,"currency":"USD"}', '{"cart_id":"C113","session":"S213"}',  'info',    '2024-01-15 09:13:00'),
    (35, 'purchase', 'web-app',     '{"item":"cable-kit","amount":19.99,"currency":"USD"}',     '{"cart_id":"C114","session":"S214"}',  'info',    '2024-01-15 09:14:00')
) AS t(id, event_type, source, payload, metadata, severity, created_at);


-- ============================================================================
-- STEP 4: Insert 10 error events
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.api_events
SELECT * FROM (VALUES
    (36, 'error', 'web-app',     '{"code":500,"message":"timeout","endpoint":"/api/data"}',        '{"trace_id":"T300","retry":1}',  'error',    '2024-01-15 10:00:00'),
    (37, 'error', 'api-gateway', '{"code":500,"message":"db connection lost","endpoint":"/api/query"}', '{"trace_id":"T301","retry":3}',  'error',    '2024-01-15 10:01:00'),
    (38, 'error', 'web-app',     '{"code":404,"message":"not found","endpoint":"/api/users/999"}', '{"trace_id":"T302","retry":0}',  'warning',  '2024-01-15 10:02:00'),
    (39, 'error', 'mobile-app',  '{"code":500,"message":"null pointer","endpoint":"/api/cart"}',   '{"trace_id":"T303","retry":2}',  'error',    '2024-01-15 10:03:00'),
    (40, 'error', 'web-app',     '{"code":403,"message":"forbidden","endpoint":"/api/admin"}',     '{"trace_id":"T304","retry":0}',  'warning',  '2024-01-15 10:04:00'),
    (41, 'error', 'api-gateway', '{"code":500,"message":"service unavailable","endpoint":"/api/search"}', '{"trace_id":"T305","retry":5}',  'error',    '2024-01-15 10:05:00'),
    (42, 'error', 'web-app',     '{"code":429,"message":"rate limited","endpoint":"/api/bulk"}',   '{"trace_id":"T306","retry":0}',  'warning',  '2024-01-15 10:06:00'),
    (43, 'error', 'mobile-app',  '{"code":500,"message":"timeout","endpoint":"/api/feed"}',        '{"trace_id":"T307","retry":2}',  'error',    '2024-01-15 10:07:00'),
    (44, 'error', 'internal',    '{"code":500,"message":"disk full","endpoint":"/api/upload"}',    '{"trace_id":"T308","retry":0}',  'error',    '2024-01-15 10:08:00'),
    (45, 'error', 'web-app',     '{"code":502,"message":"bad gateway","endpoint":"/api/proxy"}',   '{"trace_id":"T309","retry":1}',  'warning',  '2024-01-15 10:09:00')
) AS t(id, event_type, source, payload, metadata, severity, created_at);


-- ============================================================================
-- STEP 5: Insert 5 system events
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.api_events
SELECT * FROM (VALUES
    (46, 'system', 'internal',    '{"action":"restart","component":"cache","duration_ms":450}',     '{"triggered_by":"scheduler","env":"prod"}',   'info',    '2024-01-15 11:00:00'),
    (47, 'system', 'internal',    '{"action":"deploy","component":"api-server","duration_ms":12000}','{"triggered_by":"ci-cd","env":"prod"}',      'info',    '2024-01-15 11:05:00'),
    (48, 'system', 'internal',    '{"action":"backup","component":"database","duration_ms":35000}', '{"triggered_by":"scheduler","env":"prod"}',   'info',    '2024-01-15 11:10:00'),
    (49, 'system', 'internal',    '{"action":"scale-up","component":"workers","duration_ms":8000}', '{"triggered_by":"auto-scaler","env":"prod"}', 'warning', '2024-01-15 11:15:00'),
    (50, 'system', 'internal',    '{"action":"purge","component":"logs","duration_ms":2200}',       '{"triggered_by":"scheduler","env":"prod"}',   'info',    '2024-01-15 11:20:00')
) AS t(id, event_type, source, payload, metadata, severity, created_at);


