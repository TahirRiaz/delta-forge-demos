-- ============================================================================
-- Delta Protocol & Table Features — Setup Script
-- ============================================================================
-- Creates two Delta tables with different TBLPROPERTIES configurations:
--   1. feature_demo — 30 rows with CDC enabled
--   2. audit_trail  — 25 rows with append-only + CDC enabled
--
-- The queries.sql file then performs UPDATE and DELETE operations on
-- feature_demo to demonstrate how each table feature behaves.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: feature_demo — CDC + deletion vectors enabled
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.feature_demo (
    id              INT,
    name            VARCHAR,
    category        VARCHAR,
    value           DOUBLE,
    status          VARCHAR,
    created_date    VARCHAR
) LOCATION '{{data_path}}/feature_demo'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);

-- Baseline data: 30 rows, all status = 'active' or 'trial'
INSERT INTO {{zone_name}}.delta_demos.feature_demo VALUES
    (1,  'Alpha Widget',     'hardware',  150.00, 'active',       '2025-01-01'),
    (2,  'Beta Service',     'software',  299.99, 'active',       '2025-01-02'),
    (3,  'Gamma License',    'software',  499.00, 'active',       '2025-01-03'),
    (4,  'Delta Module',     'hardware',  89.50,  'active',       '2025-01-04'),
    (5,  'Epsilon Pack',     'services',  750.00, 'active',       '2025-01-05'),
    (6,  'Zeta Component',   'hardware',  45.00,  'active',       '2025-01-06'),
    (7,  'Eta Framework',    'software',  199.99, 'trial',        '2025-01-07'),
    (8,  'Theta Platform',   'software',  899.00, 'active',       '2025-01-08'),
    (9,  'Iota Adapter',     'hardware',  35.00,  'active',       '2025-01-09'),
    (10, 'Kappa Plugin',     'software',  59.99,  'trial',        '2025-01-10'),
    (11, 'Lambda Toolkit',   'services',  450.00, 'active',       '2025-01-11'),
    (12, 'Mu Connector',     'hardware',  120.00, 'active',       '2025-01-12'),
    (13, 'Nu Dashboard',     'software',  349.99, 'active',       '2025-01-13'),
    (14, 'Xi Gateway',       'hardware',  275.00, 'active',       '2025-01-14'),
    (15, 'Omicron Suite',    'software',  599.00, 'trial',        '2025-01-15'),
    (16, 'Pi Analytics',     'services',  650.00, 'active',       '2025-01-16'),
    (17, 'Rho Monitor',      'hardware',  185.00, 'active',       '2025-01-17'),
    (18, 'Sigma Firewall',   'hardware',  425.00, 'active',       '2025-01-18'),
    (19, 'Tau Scheduler',    'software',  149.99, 'active',       '2025-01-19'),
    (20, 'Upsilon Cache',    'software',  249.00, 'active',       '2025-01-20'),
    (21, 'Phi Balancer',     'hardware',  550.00, 'active',       '2025-01-21'),
    (22, 'Chi Debugger',     'software',  79.99,  'trial',        '2025-01-22'),
    (23, 'Psi Profiler',     'services',  380.00, 'active',       '2025-01-23'),
    (24, 'Omega Cluster',    'hardware',  999.00, 'active',       '2025-01-24'),
    (25, 'Alpha-2 Widget',   'hardware',  165.00, 'active',       '2025-01-25'),
    (26, 'Beta-2 Service',   'software',  319.99, 'active',       '2025-01-26'),
    (27, 'Gamma-2 License',  'services',  520.00, 'active',       '2025-01-27'),
    (28, 'Delta-2 Module',   'hardware',  95.00,  'active',       '2025-01-28'),
    (29, 'Epsilon-2 Pack',   'services',  780.00, 'active',       '2025-01-29'),
    (30, 'Zeta-2 Component', 'hardware',  55.00,  'trial',        '2025-01-30');

DETECT SCHEMA FOR TABLE {{zone_name}}.delta_demos.feature_demo;
GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.feature_demo TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: audit_trail — append-only with CDC for compliance
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.audit_trail (
    id              BIGINT,
    event_type      VARCHAR,
    actor           VARCHAR,
    resource        VARCHAR,
    action          VARCHAR,
    timestamp_utc   VARCHAR
) LOCATION '{{data_path}}/audit_trail'
TBLPROPERTIES (
    'delta.appendOnly' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

-- 25 audit events
INSERT INTO {{zone_name}}.delta_demos.audit_trail VALUES
    (1,  'AUTH',    'alice@corp.com',   'login-portal',    'LOGIN_SUCCESS',      '2025-01-01 08:00:00'),
    (2,  'AUTH',    'bob@corp.com',     'login-portal',    'LOGIN_SUCCESS',      '2025-01-01 08:05:00'),
    (3,  'DATA',    'alice@corp.com',   'customer-db',     'QUERY_EXECUTE',      '2025-01-01 08:10:00'),
    (4,  'DATA',    'alice@corp.com',   'customer-db',     'EXPORT_CSV',         '2025-01-01 08:15:00'),
    (5,  'ADMIN',   'carol@corp.com',   'user-mgmt',       'CREATE_USER',        '2025-01-01 09:00:00'),
    (6,  'AUTH',    'dave@corp.com',    'login-portal',    'LOGIN_FAILED',       '2025-01-01 09:05:00'),
    (7,  'AUTH',    'dave@corp.com',    'login-portal',    'LOGIN_FAILED',       '2025-01-01 09:06:00'),
    (8,  'AUTH',    'dave@corp.com',    'login-portal',    'ACCOUNT_LOCKED',     '2025-01-01 09:07:00'),
    (9,  'ADMIN',   'carol@corp.com',   'user-mgmt',       'UNLOCK_ACCOUNT',     '2025-01-01 09:30:00'),
    (10, 'AUTH',    'dave@corp.com',    'login-portal',    'LOGIN_SUCCESS',      '2025-01-01 09:35:00'),
    (11, 'DATA',    'bob@corp.com',     'orders-db',       'QUERY_EXECUTE',      '2025-01-01 10:00:00'),
    (12, 'DATA',    'bob@corp.com',     'orders-db',       'UPDATE_RECORD',      '2025-01-01 10:05:00'),
    (13, 'SECURITY','system',           'firewall',        'RULE_UPDATED',       '2025-01-01 10:30:00'),
    (14, 'SECURITY','system',           'ids',             'ALERT_TRIGGERED',    '2025-01-01 10:35:00'),
    (15, 'DATA',    'alice@corp.com',   'analytics-db',    'REPORT_GENERATED',   '2025-01-01 11:00:00'),
    (16, 'ADMIN',   'carol@corp.com',   'config',          'SETTING_CHANGED',    '2025-01-01 11:30:00'),
    (17, 'AUTH',    'eve@corp.com',     'login-portal',    'LOGIN_SUCCESS',      '2025-01-01 12:00:00'),
    (18, 'DATA',    'eve@corp.com',     'customer-db',     'QUERY_EXECUTE',      '2025-01-01 12:05:00'),
    (19, 'DATA',    'eve@corp.com',     'customer-db',     'DELETE_RECORD',      '2025-01-01 12:10:00'),
    (20, 'SECURITY','system',           'dlp',             'SENSITIVE_DATA_ALERT','2025-01-01 12:11:00'),
    (21, 'AUTH',    'alice@corp.com',   'login-portal',    'LOGOUT',             '2025-01-01 17:00:00'),
    (22, 'AUTH',    'bob@corp.com',     'login-portal',    'LOGOUT',             '2025-01-01 17:30:00'),
    (23, 'AUTH',    'eve@corp.com',     'login-portal',    'LOGOUT',             '2025-01-01 18:00:00'),
    (24, 'ADMIN',   'carol@corp.com',   'backup-svc',      'BACKUP_COMPLETED',   '2025-01-01 23:00:00'),
    (25, 'SECURITY','system',           'siem',            'DAILY_REPORT',       '2025-01-01 23:59:00');

DETECT SCHEMA FOR TABLE {{zone_name}}.delta_demos.audit_trail;
GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.audit_trail TO USER {{current_user}};
