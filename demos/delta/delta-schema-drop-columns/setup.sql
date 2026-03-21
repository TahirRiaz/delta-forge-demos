-- ============================================================================
-- Delta Schema Evolution — Drop Columns & GDPR Cleanup — Setup Script
-- ============================================================================
-- Creates the user_profiles table with 40 users including PII columns
-- (phone, address). The queries.sql script performs GDPR erasure operations.
--
-- Tables created:
--   1. user_profiles — 40 users with PII data
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: user_profiles — 40 users with PII columns
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.user_profiles (
    id              INT,
    username        VARCHAR,
    email           VARCHAR,
    phone           VARCHAR,
    address         VARCHAR,
    city            VARCHAR,
    country         VARCHAR,
    signup_date     VARCHAR,
    last_login      VARCHAR,
    preferences     VARCHAR
) LOCATION '{{data_path}}/user_profiles';

-- STEP 2: Insert 40 users with full PII
INSERT INTO {{zone_name}}.delta_demos.user_profiles VALUES
    (1,  'alice_dev',      'alice@example.com',     '+1-555-0101', '123 Oak St',         'San Jose',      'US', '2023-01-15', '2025-03-01', 'theme=dark,lang=en'),
    (2,  'bob_admin',      'bob@example.com',       '+1-555-0102', '456 Elm Ave',        'Chicago',       'US', '2023-02-20', '2025-03-02', 'theme=light,lang=en'),
    (3,  'carol_pm',       'carol@example.com',     '+1-555-0103', '789 Pine Rd',        'Boston',        'US', '2023-03-10', '2025-02-28', 'theme=dark,lang=en'),
    (4,  'david_eng',      'david@example.com',     '+44-20-7946-0104', '10 Baker St',   'London',        'UK', '2023-04-05', '2025-03-01', 'theme=auto,lang=en'),
    (5,  'eva_design',     'eva@example.com',       '+49-30-0105',      '15 Berliner Str','Berlin',       'DE', '2023-05-12', '2025-02-25', 'theme=dark,lang=de'),
    (6,  'frank_data',     'frank@example.com',     '+33-1-0106',       '22 Rue de Paris','Paris',        'FR', '2023-06-01', '2025-03-03', 'theme=light,lang=fr'),
    (7,  'grace_sales',    'grace@example.com',     '+1-555-0107', '88 Market St',       'New York',      'US', '2023-06-15', '2025-02-20', 'theme=dark,lang=en'),
    (8,  'henry_ops',      'henry@example.com',     '+81-3-0108',       '5 Shibuya Ku',  'Tokyo',        'JP', '2023-07-01', '2025-03-01', 'theme=auto,lang=ja'),
    (9,  'irene_qa',       'irene@example.com',     '+1-555-0109', '42 Test Ave',        'Seattle',       'US', '2023-07-20', '2025-02-28', 'theme=light,lang=en'),
    (10, 'jack_devops',    'jack@example.com',      '+61-2-0110',       '7 Harbour Rd',  'Sydney',        'AU', '2023-08-10', '2025-03-02', 'theme=dark,lang=en'),
    (11, 'karen_hr',       'karen@example.com',     '+1-555-0111', '100 People Blvd',    'Austin',        'US', '2023-08-25', '2025-02-15', 'theme=light,lang=en'),
    (12, 'leo_finance',    'leo@example.com',       '+852-0112',        '18 Central Ave', 'Hong Kong',    'HK', '2023-09-01', '2025-03-03', 'theme=dark,lang=zh'),
    (13, 'maria_mktg',     'maria@example.com',     '+34-91-0113',      '30 Gran Via',   'Madrid',        'ES', '2023-09-15', '2025-02-28', 'theme=auto,lang=es'),
    (14, 'nick_arch',      'nick@example.com',      '+1-555-0114', '55 Cloud Way',       'Portland',      'US', '2023-10-01', '2025-03-01', 'theme=dark,lang=en'),
    (15, 'olivia_lead',    'olivia@example.com',    '+46-8-0115',       '12 Gamla Stan',  'Stockholm',    'SE', '2023-10-20', '2025-02-25', 'theme=light,lang=sv'),
    (16, 'peter_ml',       'peter@example.com',     '+1-555-0116', '77 Neural Dr',       'San Francisco', 'US', '2023-11-01', '2025-03-02', 'theme=dark,lang=en'),
    (17, 'quinn_sec',      'quinn@example.com',     '+1-555-0117', '33 Cipher St',       'Denver',        'US', '2023-11-15', '2025-03-01', 'theme=auto,lang=en'),
    (18, 'rachel_ux',      'rachel@example.com',    '+972-3-0118',      '8 Rothschild',  'Tel Aviv',      'IL', '2023-12-01', '2025-02-28', 'theme=dark,lang=he'),
    (19, 'sam_backend',    'sam@example.com',       '+1-555-0119', '90 API Lane',        'Miami',         'US', '2023-12-10', '2025-03-03', 'theme=light,lang=en'),
    (20, 'tina_frontend',  'tina@example.com',      '+91-22-0120',      '25 MG Road',    'Mumbai',        'IN', '2024-01-05', '2025-02-20', 'theme=dark,lang=hi');

INSERT INTO {{zone_name}}.delta_demos.user_profiles
SELECT * FROM (VALUES
    (21, 'uma_analyst',    'uma@example.com',       '+1-555-0121', '60 Data Dr',         'Nashville',     'US', '2024-01-15', '2025-03-01', 'theme=light,lang=en'),
    (22, 'victor_sre',     'victor@example.com',    '+55-11-0122',      '40 Paulista Ave','Sao Paulo',    'BR', '2024-02-01', '2025-02-28', 'theme=auto,lang=pt'),
    (23, 'wendy_pm',       'wendy@example.com',     '+1-555-0123', '15 Sprint Rd',       'Atlanta',       'US', '2024-02-15', '2025-03-02', 'theme=dark,lang=en'),
    (24, 'xavier_db',      'xavier@example.com',    '+1-555-0124', '28 Query Blvd',      'Dallas',        'US', '2024-03-01', '2025-03-01', 'theme=light,lang=en'),
    (25, 'yuki_infra',     'yuki@example.com',      '+81-6-0125',       '11 Namba St',   'Osaka',         'JP', '2024-03-10', '2025-02-25', 'theme=dark,lang=ja'),
    (26, 'zara_legal',     'zara@example.com',      '+44-20-0126',      '5 Fleet St',    'London',        'UK', '2024-04-01', '2025-03-03', 'theme=auto,lang=en'),
    (27, 'aaron_cto',      'aaron@example.com',     '+1-555-0127', '1 Executive Dr',     'Palo Alto',     'US', '2024-04-15', '2025-03-02', 'theme=dark,lang=en'),
    (28, 'beth_support',   'beth@example.com',      '+353-1-0128',      '9 Temple Bar',  'Dublin',        'IE', '2024-05-01', '2025-02-28', 'theme=light,lang=en'),
    (29, 'chris_mobile',   'chris@example.com',     '+1-555-0129', '44 Swift Ave',       'Cupertino',     'US', '2024-05-15', '2025-03-01', 'theme=dark,lang=en'),
    (30, 'diana_ai',       'diana@example.com',     '+82-2-0130',       '20 Gangnam Ro', 'Seoul',         'KR', '2024-06-01', '2025-03-03', 'theme=auto,lang=ko'),
    (31, 'ed_cloud',       'ed@example.com',        '+1-555-0131', '66 Lambda Way',      'Phoenix',       'US', '2024-06-10', '2025-02-20', 'theme=light,lang=en'),
    (32, 'fiona_test',     'fiona@example.com',     '+64-9-0132',       '14 Queen St',   'Auckland',      'NZ', '2024-07-01', '2025-03-02', 'theme=dark,lang=en'),
    (33, 'george_devrel',  'george@example.com',    '+1-555-0133', '8 Advocate Rd',      'Raleigh',       'US', '2024-07-15', '2025-02-28', 'theme=auto,lang=en'),
    (34, 'hannah_bi',      'hannah@example.com',    '+47-22-0134',      '3 Fjord Ave',   'Oslo',          'NO', '2024-08-01', '2025-03-01', 'theme=dark,lang=no'),
    (35, 'ian_platform',   'ian@example.com',       '+1-555-0135', '52 Container Dr',    'Charlotte',     'US', '2024-08-15', '2025-03-03', 'theme=light,lang=en'),
    (36, 'julia_research', 'julia@example.com',     '+41-44-0136',      '6 Bahnhofstr',  'Zurich',        'CH', '2024-09-01', '2025-02-25', 'theme=dark,lang=de'),
    (37, 'kyle_gaming',    'kyle@example.com',      '+1-555-0137', '71 Pixel Ln',        'Los Angeles',   'US', '2024-09-15', '2025-03-02', 'theme=auto,lang=en'),
    (38, 'laura_embedded', 'laura@example.com',     '+358-9-0138',      '2 Nokia Ave',   'Helsinki',      'FI', '2024-10-01', '2025-02-28', 'theme=light,lang=fi'),
    (39, 'mike_network',   'mike@example.com',      '+1-555-0139', '39 Packet Rd',       'San Diego',     'US', '2024-10-15', '2025-03-01', 'theme=dark,lang=en'),
    (40, 'nina_product',   'nina@example.com',      '+31-20-0140',      '17 Keizersgr',  'Amsterdam',     'NL', '2024-11-01', '2025-03-03', 'theme=auto,lang=nl')
) AS t(id, username, email, phone, address, city, country, signup_date, last_login, preferences);

DETECT SCHEMA FOR TABLE {{zone_name}}.delta_demos.user_profiles;
GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.user_profiles TO USER {{current_user}};
