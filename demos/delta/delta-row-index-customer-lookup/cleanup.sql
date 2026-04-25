-- Cleanup: Online Retail Customer Lookup with Row-Level Index

DROP INDEX IF EXISTS idx_customer_id ON TABLE {{zone_name}}.delta_demos.customers;

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.customers WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
