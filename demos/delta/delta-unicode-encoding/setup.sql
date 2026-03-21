-- ============================================================================
-- Delta Unicode & Encoding — International Data — Setup Script
-- ============================================================================
-- Demonstrates Unicode and international character support:
--   - Multi-script product names (CJK, Arabic, Cyrillic, Latin diacritics)
--   - Partitioning with international data
--   - Unicode string matching and filtering
--
-- Tables created:
--   1. global_products — 50 products across 5 regions with Unicode names
--
-- Operations performed:
--   1. CREATE DELTA TABLE PARTITIONED BY (region)
--   2. INSERT — 50 products (10 per region) with local script names
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: global_products — international product catalog
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.global_products (
    id                  INT,
    product_name        VARCHAR,
    product_name_local  VARCHAR,
    category            VARCHAR,
    price               DOUBLE,
    currency            VARCHAR,
    country             VARCHAR,
    region              VARCHAR
) PARTITIONED BY (region)
  LOCATION '{{data_path}}/global_products';

-- STEP 2: Insert 50 products across 5 regions

-- Asia (10 products)
INSERT INTO {{zone_name}}.delta_demos.global_products VALUES
    (1,  'Tokyo Tower Model',       '東京タワー模型',        'souvenirs',    25.00,  'JPY', 'Japan',        'Asia'),
    (2,  'Peking Duck Sauce',       '北京烤鸭酱',            'food',         12.50,  'CNY', 'China',        'Asia'),
    (3,  'Kimchi Premium',          '김치 프리미엄',          'food',         8.99,   'KRW', 'South Korea',  'Asia'),
    (4,  'Green Tea Matcha',        '抹茶グリーンティー',     'beverages',    15.00,  'JPY', 'Japan',        'Asia'),
    (5,  'Silk Scarf Shanghai',     '上海丝绸围巾',           'clothing',     45.00,  'CNY', 'China',        'Asia'),
    (6,  'Korean Ginseng Extract',  '고려인삼 추출물',        'health',       35.00,  'KRW', 'South Korea',  'Asia'),
    (7,  'Origami Paper Set',       '折り紙セット',           'crafts',       7.50,   'JPY', 'Japan',        'Asia'),
    (8,  'Jasmine Rice Premium',    '茉莉香米特级',           'food',         18.00,  'THB', 'Thailand',     'Asia'),
    (9,  'Bamboo Chopsticks',       '竹筷子',                 'kitchenware',  3.50,   'CNY', 'China',        'Asia'),
    (10, 'Soju Classic',            '소주 클래식',             'beverages',    6.00,   'KRW', 'South Korea',  'Asia');

-- Europe (10 products)
INSERT INTO {{zone_name}}.delta_demos.global_products VALUES
    (11, 'German Sausage',          'Würstchen',              'food',         9.50,   'EUR', 'Germany',      'Europe'),
    (12, 'Creme Brulee Mix',        'Crème brûlée',           'food',         7.25,   'EUR', 'France',       'Europe'),
    (13, 'Jalapeno Hot Sauce',      'Jalapeño Salsa Picante', 'food',         4.99,   'EUR', 'Spain',        'Europe'),
    (14, 'Swiss Chocolate',         'Schweizer Schokolade',   'food',         12.00,  'CHF', 'Switzerland',  'Europe'),
    (15, 'Italian Espresso',        'Caffè Espresso',         'beverages',    8.50,   'EUR', 'Italy',        'Europe'),
    (16, 'Dutch Stroopwafel',       'Stroopwafel',            'food',         5.75,   'EUR', 'Netherlands',  'Europe'),
    (17, 'Czech Crystal Glass',     'Český Křišťál',          'crafts',       55.00,  'CZK', 'Czech Republic','Europe'),
    (18, 'Norwegian Salmon',        'Norsk Laks',             'food',         22.00,  'NOK', 'Norway',       'Europe'),
    (19, 'Greek Olive Oil',         'Ελληνικό Ελαιόλαδο',     'food',         18.50,  'EUR', 'Greece',       'Europe'),
    (20, 'Polish Pottery',          'Polska Ceramika',        'crafts',       35.00,  'PLN', 'Poland',       'Europe');

-- Americas (10 products)
INSERT INTO {{zone_name}}.delta_demos.global_products VALUES
    (21, 'Sao Paulo Coffee',        'Café São Paulo',         'beverages',    14.00,  'BRL', 'Brazil',       'Americas'),
    (22, 'Mexican Mole Sauce',      'Mole Oaxaqueño',        'food',         11.50,  'MXN', 'Mexico',       'Americas'),
    (23, 'Maple Syrup Quebec',      'Sirop d''érable',        'food',         16.00,  'CAD', 'Canada',       'Americas'),
    (24, 'Argentine Yerba Mate',    'Yerba Mate Argentina',   'beverages',    9.00,   'ARS', 'Argentina',    'Americas'),
    (25, 'Peruvian Quinoa',         'Quinua Peruana',         'food',         13.50,  'PEN', 'Peru',         'Americas'),
    (26, 'Colombian Coffee Beans',  'Café Colombiano',        'beverages',    17.50,  'COP', 'Colombia',     'Americas'),
    (27, 'Chilean Wine Reserve',    'Vino Reserva Chileno',   'beverages',    28.00,  'CLP', 'Chile',        'Americas'),
    (28, 'Jamaican Jerk Spice',     'Jamaican Jerk Seasoning','food',         6.50,   'JMD', 'Jamaica',      'Americas'),
    (29, 'US Craft Bourbon',        'Small Batch Bourbon',    'beverages',    42.00,  'USD', 'United States', 'Americas'),
    (30, 'Cuban Cigar Box',         'Caja de Puros Cubanos',  'luxury',       85.00,  'CUP', 'Cuba',         'Americas');

-- Africa (10 products)
INSERT INTO {{zone_name}}.delta_demos.global_products VALUES
    (31, 'Ethiopian Coffee',        'Yirgacheffe Buna',       'beverages',    19.00,  'ETB', 'Ethiopia',     'Africa'),
    (32, 'Kenyan Tea Premium',      'Chai ya Kenya',          'beverages',    10.50,  'KES', 'Kenya',        'Africa'),
    (33, 'Moroccan Argan Oil',      'زيت الأرغان المغربي',    'beauty',       32.00,  'MAD', 'Morocco',      'Africa'),
    (34, 'South African Rooibos',   'Rooibos Tee',            'beverages',    8.00,   'ZAR', 'South Africa', 'Africa'),
    (35, 'Nigerian Palm Oil',       'Epo Pupa',               'food',         7.50,   'NGN', 'Nigeria',      'Africa'),
    (36, 'Tanzanian Vanilla',       'Vanila ya Tanzania',     'spices',       25.00,  'TZS', 'Tanzania',     'Africa'),
    (37, 'Egyptian Cotton Sheet',   'ملاءة قطن مصري',         'textiles',     45.00,  'EGP', 'Egypt',        'Africa'),
    (38, 'Ghanaian Cocoa Butter',   'Cocoa Butter Ghana',     'food',         14.00,  'GHS', 'Ghana',        'Africa'),
    (39, 'Tunisian Dates',          'تمور تونسية',            'food',         11.00,  'TND', 'Tunisia',      'Africa'),
    (40, 'Madagascar Pepper',       'Poivre de Madagascar',   'spices',       3.50,   'MGA', 'Madagascar',   'Africa');

-- MiddleEast (10 products)
INSERT INTO {{zone_name}}.delta_demos.global_products VALUES
    (41, 'Turkish Delight',         'Türk Lokumu',            'food',         9.00,   'TRY', 'Turkey',       'MiddleEast'),
    (42, 'Lebanese Falafel Mix',    'فلافل لبناني',           'food',         6.50,   'LBP', 'Lebanon',      'MiddleEast'),
    (43, 'Iranian Saffron',         'زعفران ایرانی',          'spices',       75.00,  'IRR', 'Iran',         'MiddleEast'),
    (44, 'Israeli Hummus',          'חומוס ישראלי',           'food',         5.50,   'ILS', 'Israel',       'MiddleEast'),
    (45, 'Dubai Gold Dates',        'تمور ذهبية دبي',        'food',         20.00,  'AED', 'UAE',          'MiddleEast'),
    (46, 'Omani Frankincense',      'لبان عماني',             'fragrance',    15.00,  'OMR', 'Oman',         'MiddleEast'),
    (47, 'Jordanian Za''atar',      'زعتر أردني',             'spices',       8.50,   'JOD', 'Jordan',       'MiddleEast'),
    (48, 'Bahraini Pearl',          'لؤلؤ بحريني',            'jewelry',      120.00, 'BHD', 'Bahrain',      'MiddleEast'),
    (49, 'Kuwaiti Cardamom Coffee', 'قهوة كويتية بالهيل',     'beverages',    12.00,  'KWD', 'Kuwait',       'MiddleEast'),
    (50, 'Istanbul Carpet Sample',  'İstanbul Halı Örneği',   'textiles',     65.00,  'TRY', 'Turkey',       'MiddleEast');

DETECT SCHEMA FOR TABLE {{zone_name}}.delta_demos.global_products;
GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.global_products TO USER {{current_user}};
