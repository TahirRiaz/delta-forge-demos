-- ============================================================================
-- Delta Views & Data Masking — Role-Based Access Layers — Educational Queries
-- ============================================================================
-- WHAT: Views project different column subsets from the same base table,
--       enforcing role-based data access at the SQL layer.
-- WHY:  Production databases serve analysts (need metrics, not PII), support
--       agents (need customer details with masked payment info), and
--       executives (need regional summaries, not individual records). Without
--       views, every consumer queries the full table — risking PII exposure.
-- HOW:  CREATE VIEW defines a named query. Each view selects only the columns
--       appropriate for its audience. Masking expressions (string concat)
--       redact sensitive fields inline. Aggregate views pre-compute summaries.
-- ============================================================================


-- ============================================================================
-- EXPLORE: The Full Order Table (All Columns Visible)
-- ============================================================================
-- The base table has 13 columns including sensitive fields: credit_card_last4,
-- shipping_address, phone. Everyone with table access sees everything.

ASSERT ROW_COUNT = 3
SELECT id, customer_name, credit_card_last4, shipping_address, product, order_total
FROM {{zone_name}}.delta_demos.customer_orders
WHERE id IN (1, 2, 19)
ORDER BY id;


-- ============================================================================
-- STEP 1: CREATE ANALYST VIEW — Product & Revenue Data Only (No PII)
-- ============================================================================
-- Analysts need order metrics: product, quantity, price, status, date, region.
-- They never need customer names, emails, payment, or addresses.
-- This view projects only the non-sensitive analytical columns.

CREATE VIEW {{zone_name}}.delta_demos.orders_analyst AS
SELECT id, product, quantity, unit_price, order_total, order_status, order_date, region
FROM {{zone_name}}.delta_demos.customer_orders;


-- ============================================================================
-- EXPLORE: Analyst View — Clean Revenue Data, Zero PII
-- ============================================================================
-- The analyst sees product, pricing, and status — but no customer identity.

ASSERT ROW_COUNT = 3
ASSERT VALUE product = 'Laptop Pro 15' WHERE id = 1
ASSERT VALUE order_total = 1299.99 WHERE id = 1
SELECT *
FROM {{zone_name}}.delta_demos.orders_analyst
WHERE id IN (1, 2, 19)
ORDER BY id;


-- ============================================================================
-- STEP 2: CREATE SUPPORT VIEW — Customer Details with Masked Card
-- ============================================================================
-- Support agents need customer names and contact info to handle tickets,
-- but must never see full credit card numbers. The view masks the card
-- as '****-****-****-XXXX', exposing only the last 4 digits.

CREATE VIEW {{zone_name}}.delta_demos.orders_support AS
SELECT id, customer_name, customer_email,
       '****-****-****-' || credit_card_last4 AS masked_card,
       phone,
       product, order_total, order_status, order_date
FROM {{zone_name}}.delta_demos.customer_orders;


-- ============================================================================
-- EXPLORE: Support View — Masked Payment, Full Customer Context
-- ============================================================================
-- Support sees the customer name and masked card for dispute handling.

ASSERT ROW_COUNT = 2
ASSERT VALUE masked_card = '****-****-****-4532' WHERE id = 1
SELECT id, customer_name, masked_card, order_status
FROM {{zone_name}}.delta_demos.orders_support
WHERE id IN (1, 19)
ORDER BY id;


-- ============================================================================
-- STEP 3: CREATE EXECUTIVE VIEW — Aggregated Regional Summaries
-- ============================================================================
-- Executives need high-level KPIs: revenue per region, order counts, return
-- rates. They should never see individual customer records or transactions.
-- This view pre-aggregates by region, eliminating row-level detail entirely.

CREATE VIEW {{zone_name}}.delta_demos.orders_executive AS
SELECT region,
       COUNT(*) AS total_orders,
       SUM(order_total) AS revenue,
       COUNT(DISTINCT customer_name) AS unique_customers,
       COUNT(*) FILTER (WHERE order_status = 'returned') AS returns
FROM {{zone_name}}.delta_demos.customer_orders
GROUP BY region;


-- ============================================================================
-- EXPLORE: Executive View — Regional KPIs at a Glance
-- ============================================================================
-- 6 regions, each with pre-computed revenue and return counts.

ASSERT ROW_COUNT = 6
ASSERT VALUE total_orders = 13 WHERE region = 'Europe'
SELECT *
FROM {{zone_name}}.delta_demos.orders_executive
ORDER BY revenue DESC;


-- ============================================================================
-- LEARN: Revenue by Product (Through Analyst View)
-- ============================================================================
-- The analyst view supports full aggregation on the projected columns.
-- No PII leaks even when analysts run ad-hoc GROUP BY queries.

ASSERT ROW_COUNT = 7
ASSERT VALUE product_revenue = 9099.93 WHERE product = 'Laptop Pro 15'
SELECT product, COUNT(*) AS orders, SUM(order_total) AS product_revenue
FROM {{zone_name}}.delta_demos.orders_analyst
GROUP BY product
ORDER BY product_revenue DESC;


-- ============================================================================
-- LEARN: Order Status Distribution (Through Analyst View)
-- ============================================================================
-- Status breakdown across all orders — the same data an operations team
-- would use for fulfillment dashboards.

ASSERT ROW_COUNT = 4
ASSERT VALUE status_count = 19 WHERE order_status = 'delivered'
SELECT order_status, COUNT(*) AS status_count
FROM {{zone_name}}.delta_demos.orders_analyst
GROUP BY order_status
ORDER BY status_count DESC;


-- ============================================================================
-- EXPLORE: Returns Investigation (Through Support View)
-- ============================================================================
-- Support agents investigate returns using the masked-card view.
-- They see the customer, product, and masked payment — enough context
-- to process the return without exposing the full card number.

ASSERT ROW_COUNT = 2
SELECT id, customer_name, masked_card, product, order_total, order_date
FROM {{zone_name}}.delta_demos.orders_support
WHERE order_status = 'returned'
ORDER BY order_date;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify base table has 30 rows
ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.delta_demos.customer_orders;

-- Verify analyst view has 30 rows
ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.delta_demos.orders_analyst;

-- Verify support view has 30 rows
ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.delta_demos.orders_support;

-- Verify executive view has 6 regions
ASSERT VALUE region_count = 6
SELECT COUNT(*) AS region_count FROM {{zone_name}}.delta_demos.orders_executive;

-- Verify total revenue
ASSERT VALUE total_revenue = 15582.61
SELECT SUM(order_total) AS total_revenue FROM {{zone_name}}.delta_demos.customer_orders;

-- Verify status counts
ASSERT VALUE delivered_count = 19
SELECT COUNT(*) AS delivered_count FROM {{zone_name}}.delta_demos.customer_orders WHERE order_status = 'delivered';

ASSERT VALUE returned_count = 2
SELECT COUNT(*) AS returned_count FROM {{zone_name}}.delta_demos.customer_orders WHERE order_status = 'returned';

ASSERT VALUE pending_count = 3
SELECT COUNT(*) AS pending_count FROM {{zone_name}}.delta_demos.customer_orders WHERE order_status = 'pending';

ASSERT VALUE shipped_count = 6
SELECT COUNT(*) AS shipped_count FROM {{zone_name}}.delta_demos.customer_orders WHERE order_status = 'shipped';

-- Verify 7 distinct products
ASSERT VALUE product_count = 7
SELECT COUNT(DISTINCT product) AS product_count FROM {{zone_name}}.delta_demos.customer_orders;

-- Verify analyst view revenue matches base table
ASSERT VALUE analyst_revenue = 15582.61
SELECT SUM(order_total) AS analyst_revenue FROM {{zone_name}}.delta_demos.orders_analyst;
