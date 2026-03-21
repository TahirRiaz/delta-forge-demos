# Delta Schema Evolution — Drop Columns & GDPR Cleanup

Demonstrates GDPR-style data erasure patterns using UPDATE to NULL out
PII columns and CREATE VIEW to project only non-PII data.

## Data Story

A SaaS platform stores user profiles with PII (phone, address). When 15
users exercise their GDPR right to erasure, their PII columns are NULLed
out while preserving non-PII data. A clean view provides safe access
to the remaining data for analytics.

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `user_profiles` | Delta Table | 40 | User profiles with PII columns |
| `user_profiles_clean` | View | 40 | Non-PII projection (no phone/address) |

## Schema

**user_profiles:** `id INT, username VARCHAR, email VARCHAR, phone VARCHAR, address VARCHAR, city VARCHAR, country VARCHAR, signup_date VARCHAR, last_login VARCHAR, preferences VARCHAR`

## Patterns Demonstrated

1. **PII identification** — phone and address as erasable columns
2. **GDPR erasure** — UPDATE SET NULL for targeted users
3. **Clean views** — CREATE VIEW excluding PII columns
4. **Data integrity** — non-PII fields preserved after erasure
5. **Selective erasure** — only requested users affected

## Verification

8 automated PASS/FAIL checks verify row counts, NULL counts for erased
columns, intact non-GDPR users, view accessibility, preserved non-PII
data, and country distribution.
