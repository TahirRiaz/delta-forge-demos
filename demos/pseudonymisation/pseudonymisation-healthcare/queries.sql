-- ============================================================================
-- Pseudonymisation Healthcare — Demo Queries
-- ============================================================================
-- Queries showcasing GDPR-compliant pseudonymisation across HL7, FHIR, and
-- EDI healthcare data. Demonstrates all 5 pseudonymisation commands:
--   CREATE RULE  — (done in setup.sql)
--   SHOW RULES   — review active protection rules
--   SELECT       — query pseudonymised data at runtime
--   ALTER RULE   — enable/disable rules for auditing
--   APPLY        — permanent data transformation
--   DROP RULE    — remove individual rules
--
-- Three tables are available:
--   hl7_patients   — HL7 v2 ADT with PID fields (4 patients)
--   fhir_patients  — FHIR R4 Patient resources (4 patients)
--   edi_claims     — EDI HIPAA X12 transactions (5 claims)
--
-- Transform types used:
--   keyed_hash  — Deterministic hash with salt (linkable pseudonym)
--   encrypt     — Reversible encryption (needs key to decrypt)
--   redact      — Full replacement with mask string
--   generalize  — Reduce precision (DOB → year, year → decade)
--   tokenize    — Opaque token (TOK_ prefix)
--   mask        — Partial visibility (first N characters)
--   hash        — One-way SHA256 fingerprint (no salt)
-- ============================================================================


-- ============================================================================
-- 1. Review All Rules — SHOW PSEUDONYMISATION RULES
-- ============================================================================
-- Lists every pseudonymisation rule across all three tables. Each row shows
-- the table, column pattern, pattern type (exact/wildcard), transform type,
-- linkability scope, priority, and whether the rule is currently enabled.
--
-- What you'll see:
--   - 22 rules total (6 HL7, 8 FHIR, 8 EDI)
--   - All 7 transform types represented
--   - Wildcard patterns: address_* and *_name on FHIR table
--
-- Expected: 22 rows, all enabled = true

SHOW PSEUDONYMISATION RULES;


-- ============================================================================
-- 2. Rules Per Table
-- ============================================================================
-- Filter rules to a single table for focused review.

SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation.hl7_patients;

SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation.fhir_patients;

SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation.edi_claims;


-- ============================================================================
-- 3. HL7 — Query Pseudonymised Patient Admissions
-- ============================================================================
-- With rules active, SELECT queries return transformed values at runtime.
-- Original data remains untouched on disk.
--
-- What you'll see:
--   - pid_3 (MRN):   SHA256 keyed hash (deterministic per patient)
--   - pid_5 (name):  TOK_ prefixed token
--   - pid_7 (DOB):   Generalized to YYYY0000 (birth year only)
--   - pid_13 (phone): First 5 chars visible, rest masked with *
--   - pid_19 (SSN):  Fully redacted to ***-**-****
--   - pv1_2, pv1_7, status: Unchanged (no rules on these columns)
--
-- Expected: 4 rows with transformed PII, clinical fields unchanged

SELECT
    df_message_id,
    pid_3  AS mrn_hash,
    pid_5  AS name_token,
    pid_7  AS dob_generalized,
    pid_13 AS phone_masked,
    pid_19 AS ssn_redacted,
    pv1_2,
    pv1_7  AS physician,
    status
FROM {{zone_name}}.pseudonymisation.hl7_patients;


-- ============================================================================
-- 4. FHIR — Query Pseudonymised Patient Demographics
-- ============================================================================
-- Demonstrates wildcard rule effects: address_* columns are all hashed,
-- *_name columns are keyed_hash pseudonyms.
--
-- What you'll see:
--   - patient_id:  TOK_ token (tokenize, scope person)
--   - family_name: SHA256 keyed hash (*_name wildcard match)
--   - given_name:  SHA256 keyed hash (*_name wildcard match)
--   - birth_date:  Generalized to decade (1974 → 1970)
--   - gender:      Unchanged (no rule)
--   - email:       Encrypted hash (reversible with key)
--   - phone:       First 4 chars visible, rest masked
--   - mrn:         [REDACTED]
--   - ssn:         SHA256 keyed hash
--
-- Expected: 4 rows

SELECT
    patient_id   AS id_token,
    family_name  AS name_hash,
    given_name   AS name_hash2,
    birth_date   AS dob_generalized,
    gender,
    email        AS email_encrypted,
    phone        AS phone_masked,
    mrn          AS mrn_redacted,
    ssn          AS ssn_hash,
    active
FROM {{zone_name}}.pseudonymisation.fhir_patients;


-- ============================================================================
-- 5. FHIR — Verify Wildcard Rules on Address Columns
-- ============================================================================
-- The address_* wildcard rule matches all four address columns.
-- All should show SHA256 hash values instead of real addresses.
--
-- Expected: 4 rows, all address fields are 64-char hex strings

SELECT
    patient_id    AS id_token,
    address_line  AS addr_hash,
    address_city  AS city_hash,
    address_state AS state_hash,
    address_postal AS zip_hash
FROM {{zone_name}}.pseudonymisation.fhir_patients;


-- ============================================================================
-- 6. EDI — Query Pseudonymised HIPAA Claims
-- ============================================================================
-- Filters to 837 Professional Claims to show pseudonymisation of patient
-- identifiers and financial data within EDI transactions.
--
-- What you'll see:
--   - nm1_3/nm1_4: SHA256 keyed hash (patient names)
--   - nm1_8:       SHA256 keyed hash (member ID / SSN)
--   - clm_1:       TOK_ token (patient account number, scope transaction)
--   - clm_2:       First 2 digits visible, rest masked
--   - bpr_8/bpr_14: ********** (bank accounts fully redacted)
--
-- Expected: 3 rows (837 claims only)

SELECT
    df_transaction_id,
    st_1            AS txn_type,
    nm1_3           AS name_hash,
    nm1_4           AS first_hash,
    nm1_8           AS member_id_hash,
    clm_1           AS acct_token,
    clm_2           AS amount_masked,
    bpr_8           AS bank_acct_redacted,
    bpr_14          AS bank_acct2_redacted
FROM {{zone_name}}.pseudonymisation.edi_claims
WHERE st_1 = '837';


-- ============================================================================
-- 7. Aggregations on Pseudonymised Data
-- ============================================================================
-- Aggregations still work — masking and hashing do not affect SUM, AVG, COUNT.
-- This allows analytics on protected data without exposing individual records.
--
-- Expected: 2 rows (837 and 835 transaction types)

SELECT
    st_1 AS transaction_type,
    COUNT(*) AS claim_count,
    SUM(clm_2) AS total_charges,
    AVG(bpr_2) AS avg_payment
FROM {{zone_name}}.pseudonymisation.edi_claims
WHERE st_1 IN ('837', '835')
GROUP BY st_1;


-- ============================================================================
-- 8. Rule Lifecycle — ALTER (Disable / Enable)
-- ============================================================================
-- Temporarily disable SSN redaction on HL7 for a data quality audit.
-- After inspection, re-enable it.

-- Disable SSN redaction
ALTER PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.hl7_patients (pid_19) SET DISABLED;

-- Verify the rule is disabled (enabled = false for pid_19)
SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation.hl7_patients;

-- Re-enable after audit
ALTER PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.hl7_patients (pid_19) SET ENABLED;

-- Disable the FHIR address wildcard rule temporarily
ALTER PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.fhir_patients (address_*) SET DISABLED;

-- Re-enable
ALTER PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.fhir_patients (address_*) SET ENABLED;


-- ============================================================================
-- 9. Permanent Pseudonymisation — APPLY
-- ============================================================================
-- APPLY PSEUDONYMISATION permanently transforms data in place by generating
-- and executing an UPDATE statement. Use this when data must be irreversibly
-- de-identified before sharing or for right-to-erasure compliance.
--
-- IMPORTANT: Unlike CREATE RULE (which transforms at query time), APPLY
-- modifies the actual stored data. This cannot be undone.

-- Permanently redact SSN for discharged HL7 patients (data retention policy)
APPLY PSEUDONYMISATION ON {{zone_name}}.pseudonymisation.hl7_patients (pid_19)
    TRANSFORM redact
    PARAMS (mask = '***-**-****')
    WHERE status = 'Discharged';

-- Permanently hash SSN for inactive FHIR patients (GDPR right to erasure)
APPLY PSEUDONYMISATION ON {{zone_name}}.pseudonymisation.fhir_patients (ssn)
    TRANSFORM keyed_hash
    PARAMS (key = 'erasure_key_2024')
    WHERE active = false;

-- Permanently tokenize member IDs on old eligibility inquiries
APPLY PSEUDONYMISATION ON {{zone_name}}.pseudonymisation.edi_claims (nm1_8)
    TRANSFORM tokenize
    WHERE st_1 = '270';

-- Verify permanent transformations
SELECT df_message_id, pid_19, status
FROM {{zone_name}}.pseudonymisation.hl7_patients
WHERE status = 'Discharged';

SELECT patient_id, ssn, active
FROM {{zone_name}}.pseudonymisation.fhir_patients
WHERE active = false;


-- ============================================================================
-- 10. Drop Individual Rules
-- ============================================================================
-- Remove a specific rule by table and column pattern.

-- Remove the phone masking rule from HL7
DROP PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.hl7_patients (pid_13);

-- Verify removal
SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation.hl7_patients;


-- ============================================================================
-- 11. SUMMARY — Compliance Mapping
-- ============================================================================
/*
HIPAA Safe Harbor De-Identification Mapping:
+------------------------+---------------------+------------------------+---------------------------+
| HIPAA Identifier       | HL7 Column          | FHIR Column            | EDI Column                |
+------------------------+---------------------+------------------------+---------------------------+
| 1. Name                | pid_5 (tokenize)    | *_name (keyed_hash)    | nm1_3/nm1_4 (keyed_hash)  |
| 2. Address (< state)   | pid_11 (hash)       | address_* (hash)       | --                        |
| 3. Dates (except year) | pid_7 (generalize)  | birth_date (generalize)| dmg_1 (generalize)        |
| 4. Phone               | pid_13 (mask)       | phone (mask)           | --                        |
| 5. SSN                 | pid_19 (redact)     | ssn (keyed_hash)       | nm1_8 (keyed_hash)        |
| 6. MRN                 | pid_3 (keyed_hash)  | mrn (redact)           | clm_1 (tokenize)          |
| 7. Email               | --                  | email (encrypt)        | --                        |
| 8. Account numbers     | --                  | --                     | bpr_8/bpr_14 (redact)     |
+------------------------+---------------------+------------------------+---------------------------+

Transform Type Summary:
+--------------+-----------+----------------------------------------------------+
| Transform    | Reversible| Use Case                                           |
+--------------+-----------+----------------------------------------------------+
| keyed_hash   | No        | Deterministic pseudonym for linkage studies         |
| encrypt      | Yes       | Reversible protection; needs key for re-identify   |
| redact       | No        | Full removal; HIPAA Safe Harbor compliance          |
| generalize   | No        | Reduce precision (DOB -> year, age -> range)        |
| tokenize     | Yes*      | Replace with opaque token; lookup table required    |
| mask         | No        | Partial visibility (first N chars, e.g. area code)  |
| hash         | No        | One-way fingerprint; no salt (not for linkage)      |
+--------------+-----------+----------------------------------------------------+
* Tokenize reversibility depends on retention of the token mapping table.

GDPR Article 4(5) Compliance:
- All transforms produce data that cannot be attributed to a specific person
  without the use of additional information (salt, key, token map)
- Additional information is kept separately (PARAMS salts, encryption keys)
- SCOPE person ensures consistent pseudonyms for longitudinal research
- SCOPE transaction limits linkability to a single query execution
- APPLY PSEUDONYMISATION supports right-to-erasure via permanent transformation
*/
