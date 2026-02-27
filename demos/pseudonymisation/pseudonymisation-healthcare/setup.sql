-- ============================================================================
-- Pseudonymisation Healthcare — Setup Script
-- ============================================================================
-- Creates three healthcare tables (HL7, FHIR, EDI) with sample patient data
-- and applies GDPR-compliant pseudonymisation rules to sensitive columns.
--
-- Tables created:
--   1. hl7_patients   — HL7 v2 ADT patient admissions (materialized PID fields)
--   2. fhir_patients  — FHIR R4 Patient resources with demographics
--   3. edi_claims     — EDI HIPAA X12 transactions (materialized NM1/CLM/BPR)
--
-- Each table receives targeted pseudonymisation rules demonstrating all 7
-- transform types (keyed_hash, encrypt, redact, generalize, tokenize, mask,
-- hash) and all 3 scopes (person, relationship, transaction).
--
-- Compliance context:
--   HIPAA Safe Harbor  — De-identification of 18 identifier types
--   GDPR Article 4(5)  — Pseudonymisation as a safeguard measure
--   HITECH Act         — Breach notification safe harbor for encrypted PHI
--
-- Variables (auto-injected by Delta Forge):
--   zone_name     — Target zone name (defaults to 'external')
--   current_user  — Username of the current logged-in user
-- ============================================================================


-- ============================================================================
-- STEP 1: Zone & Schema
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.pseudonymisation
    COMMENT 'Pseudonymisation demo — healthcare data with protection rules';


-- ============================================================================
-- TABLE 1: hl7_patients — HL7 v2 ADT Patient Admissions
-- ============================================================================
-- Modeled on the materialized view of HL7 ADT messages. PID fields contain
-- the most common HIPAA identifiers: name, DOB, SSN, address, phone.
-- ============================================================================

CREATE TABLE IF NOT EXISTS {{zone_name}}.pseudonymisation.hl7_patients (
    df_message_id   VARCHAR PRIMARY KEY,
    pid_3           VARCHAR,          -- Patient ID / MRN
    pid_5           VARCHAR,          -- Patient name (LAST^FIRST^MIDDLE)
    pid_7           VARCHAR,          -- Date of birth (YYYYMMDD)
    pid_8           VARCHAR,          -- Gender (M/F)
    pid_11          VARCHAR,          -- Address (STREET^CITY^STATE^ZIP)
    pid_13          VARCHAR,          -- Home phone
    pid_19          VARCHAR,          -- SSN
    pv1_2           VARCHAR,          -- Patient class (I/O/E)
    pv1_3           VARCHAR,          -- Assigned location
    pv1_7           VARCHAR,          -- Attending physician
    evn_1           VARCHAR,          -- Event type code
    status          VARCHAR           -- Active/Discharged
)
COMMENT 'HL7 v2 ADT patient admissions with materialized PID fields';

INSERT INTO {{zone_name}}.pseudonymisation.hl7_patients VALUES
    ('MSG001', 'MRN-10045', 'SMITH^WILLIAM^A', '19610615', 'M', '1200 N ELM STREET^^JERUSALEM^TN^99999', '(999)999-1212', '123-45-6789', 'I', 'W4-R201-B1', 'DR JONES', 'A01', 'Active'),
    ('MSG002', 'MRN-10046', 'DOE^JANE^M', '19850322', 'F', '456 OAK AVE^^BIRMINGHAM^AL^35209', '(555)123-4567', '234-56-7890', 'O', 'CLINIC-A', 'DR PATEL', 'A04', 'Active'),
    ('MSG003', 'MRN-10047', 'KLEINSAMPLE^BARRY^Q', '19480203', 'M', '260 GOODWIN CREST^^BIRMINGHAM^AL^35209', '(555)987-6543', '345-67-8901', 'E', 'ER-BAY3', 'DR CHEN', 'A01', 'Active'),
    ('MSG004', 'MRN-10048', 'JOHNSON^ALICE^R', '19901114', 'F', '789 PINE RD^^CHICAGO^IL^60601', '(312)555-0199', '456-78-9012', 'I', 'W2-R105-B2', 'DR WILSON', 'A01', 'Discharged');

GRANT ADMIN ON TABLE {{zone_name}}.pseudonymisation.hl7_patients TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: fhir_patients — FHIR R4 Patient Resources
-- ============================================================================
-- Modeled on flattened FHIR Patient resources. Human-readable column names
-- make wildcard patterns (address_*, *_name) practical for broad protection.
-- ============================================================================

CREATE TABLE IF NOT EXISTS {{zone_name}}.pseudonymisation.fhir_patients (
    patient_id      VARCHAR PRIMARY KEY,
    family_name     VARCHAR,          -- Patient last name
    given_name      VARCHAR,          -- Patient first name
    birth_date      DATE,             -- Date of birth
    gender          VARCHAR,          -- male/female/other/unknown
    email           VARCHAR,          -- Sensitive: Contact email
    phone           VARCHAR,          -- Sensitive: Contact phone
    address_line    VARCHAR,          -- Sensitive: Street address
    address_city    VARCHAR,
    address_state   VARCHAR,
    address_postal  VARCHAR,          -- Sensitive: ZIP code
    mrn             VARCHAR,          -- Medical Record Number
    ssn             VARCHAR,          -- Sensitive: Social Security Number
    marital_status  VARCHAR,
    active          BOOLEAN
)
COMMENT 'FHIR R4 Patient resources with demographic fields';

INSERT INTO {{zone_name}}.pseudonymisation.fhir_patients VALUES
    ('pt-fhir-001', 'Chalmers', 'Peter', '1974-12-25', 'male', 'peter.chalmers@example.com', '(03) 5555 6473', '534 Erewhon St', 'PleasantVille', 'VT', '05401', 'MRN-20001', '111-22-3333', 'M', true),
    ('pt-fhir-002', 'Solo', 'Leia', '1995-10-12', 'female', 'leia.solo@hospital.org', '(555) 867-5309', '100 Galaxy Way', 'Alderaan', 'CA', '90210', 'MRN-20002', '222-33-4444', 'S', true),
    ('pt-fhir-003', 'Duck', 'Donald', '1934-06-09', 'male', 'dduck@duckburg.net', '(555) 382-5633', '1313 Webfoot Walk', 'Duckburg', 'CA', '95501', 'MRN-20003', '333-44-5555', 'M', true),
    ('pt-fhir-004', 'Doe', 'Jane', '1988-03-15', 'female', 'jdoe@clinic.net', '(555) 246-8101', '42 Unknown St', 'Springfield', 'IL', '62704', 'MRN-20004', '444-55-6666', 'S', false);

GRANT ADMIN ON TABLE {{zone_name}}.pseudonymisation.fhir_patients TO USER {{current_user}};


-- ============================================================================
-- TABLE 3: edi_claims — EDI HIPAA X12 Transactions
-- ============================================================================
-- Modeled on materialized HIPAA X12 transactions. Contains patient identifiers
-- (NM1), financial data (BPR), and claim details (CLM).
-- ============================================================================

CREATE TABLE IF NOT EXISTS {{zone_name}}.pseudonymisation.edi_claims (
    df_transaction_id VARCHAR PRIMARY KEY,
    st_1            VARCHAR,          -- Transaction type (837/835/270/271)
    bht_2           VARCHAR,          -- Transaction purpose code
    nm1_1           VARCHAR,          -- Entity ID code (IL=patient, 85=provider)
    nm1_3           VARCHAR,          -- Last name or organization
    nm1_4           VARCHAR,          -- First name
    nm1_8           VARCHAR,          -- Identifier: SSN or Member ID
    dmg_1           VARCHAR,          -- Date of birth (MMDDYYYY)
    dmg_2           VARCHAR,          -- Gender code
    clm_1           VARCHAR,          -- Patient account / claim number
    clm_2           DOUBLE,           -- Total claim charge amount
    bpr_1           VARCHAR,          -- Transaction handling code
    bpr_2           DOUBLE,           -- Total payment amount
    bpr_8           VARCHAR,          -- Sender bank account number
    bpr_14          VARCHAR           -- Receiver bank account number
)
COMMENT 'EDI HIPAA X12 transactions with materialized NM1/CLM/BPR fields';

INSERT INTO {{zone_name}}.pseudonymisation.edi_claims VALUES
    ('TXN-837-001', '837', '00', 'IL', 'SMITH', 'FRED', '123456789A', '12101930', 'M', 'ACCT-5001', 1250.00, 'C', 1250.00, '9876543210', '1234567890'),
    ('TXN-837-002', '837', '00', 'IL', 'JONES', 'MARY', '234567890A', '05151985', 'F', 'ACCT-5002', 3750.50, 'C', 3750.50, '8765432109', '2345678901'),
    ('TXN-835-001', '835', '08', '85', 'GENERAL HOSPITAL', NULL, '987654321', NULL, NULL, NULL, NULL, 'H', 5000.50, '7654321098', '3456789012'),
    ('TXN-270-001', '270', '13', 'IL', 'MANN', 'JOHN', '345678901', '07041990', 'M', NULL, NULL, NULL, NULL, NULL, NULL),
    ('TXN-837-003', '837', '00', 'IL', 'WILLIAMS', 'CAROL', '456789012A', '11301978', 'F', 'ACCT-5003', 890.00, 'C', 890.00, '6543210987', '4567890123');

GRANT ADMIN ON TABLE {{zone_name}}.pseudonymisation.edi_claims TO USER {{current_user}};


-- ============================================================================
-- STEP 2: HL7 Pseudonymisation Rules
-- ============================================================================
-- HL7 messages contain HIPAA identifiers in PID segments. These rules
-- protect patient identity while preserving clinical utility.

-- Hash the Medical Record Number (MRN) for linkable de-identification
-- SCOPE person: same MRN always produces the same hash across queries
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.hl7_patients (pid_3)
    TRANSFORM keyed_hash
    SCOPE person
    PRIORITY 10
    PARAMS (salt = 'hl7_mrn_salt_2024');

-- Redact SSN completely — HIPAA Safe Harbor requires full removal
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.hl7_patients (pid_19)
    TRANSFORM redact
    PRIORITY 20
    PARAMS (mask = '***-**-****');

-- Generalize date of birth to birth year (HIPAA: ages 0-89 allowed)
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.hl7_patients (pid_7)
    TRANSFORM generalize
    SCOPE relationship
    PARAMS (range = 10000);

-- Mask phone number — show area code only (first 5 chars)
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.hl7_patients (pid_13)
    TRANSFORM mask
    PARAMS (show = 5);

-- Hash patient address for geographic analysis without exposing location
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.hl7_patients (pid_11)
    TRANSFORM hash;

-- Tokenize patient name — replace with reversible token for re-identification
-- SCOPE person: same name always produces the same token
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.hl7_patients (pid_5)
    TRANSFORM tokenize
    SCOPE person
    PRIORITY 5;


-- ============================================================================
-- STEP 3: FHIR Pseudonymisation Rules
-- ============================================================================
-- FHIR resources use human-readable field names. Wildcard patterns
-- efficiently protect multiple columns matching a naming convention.

-- Encrypt email for reversible de-identification (needs key to decrypt)
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.fhir_patients (email)
    TRANSFORM encrypt
    SCOPE person
    PRIORITY 10
    PARAMS (algorithm = 'AES256');

-- Tokenize patient_id for cross-resource linkage without real IDs
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.fhir_patients (patient_id)
    TRANSFORM tokenize
    SCOPE person
    PRIORITY 10;

-- Hash SSN with a salt
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.fhir_patients (ssn)
    TRANSFORM keyed_hash
    SCOPE person
    PARAMS (salt = 'fhir_ssn_salt_2024');

-- Mask phone: show first 4 characters, mask rest
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.fhir_patients (phone)
    TRANSFORM mask
    PRIORITY 5
    PARAMS (show = 4);

-- Redact MRN
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.fhir_patients (mrn)
    TRANSFORM redact
    PARAMS (mask = '[REDACTED]');

-- Wildcard pattern: protect all address columns at once
-- Matches: address_line, address_city, address_state, address_postal
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.fhir_patients (address_*)
    TRANSFORM hash
    PRIORITY 1;

-- Generalize birth_date to decade
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.fhir_patients (birth_date)
    TRANSFORM generalize
    SCOPE relationship
    PARAMS (range = 10);

-- Wildcard: protect all name fields
-- Matches: family_name, given_name
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.fhir_patients (*_name)
    TRANSFORM keyed_hash
    SCOPE person
    PRIORITY 3
    PARAMS (salt = 'fhir_name_salt_2024');


-- ============================================================================
-- STEP 4: EDI / HIPAA Pseudonymisation Rules
-- ============================================================================
-- EDI X12 segments contain patient identifiers (NM1), financial data (BPR),
-- and claim details (CLM). Pseudonymisation must satisfy HIPAA Privacy Rule
-- while preserving enough structure for claims processing analytics.

-- Hash member ID / SSN — critical HIPAA identifier
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.edi_claims (nm1_8)
    TRANSFORM keyed_hash
    SCOPE person
    PRIORITY 20
    PARAMS (salt = 'edi_member_id_salt_2024');

-- Redact bank account numbers completely
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.edi_claims (bpr_8)
    TRANSFORM redact
    PRIORITY 20
    PARAMS (mask = '**********');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.edi_claims (bpr_14)
    TRANSFORM redact
    PRIORITY 20
    PARAMS (mask = '**********');

-- Tokenize patient account / claim number
-- SCOPE transaction: each query execution generates a different token,
-- preventing linkage across separate analyst sessions
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.edi_claims (clm_1)
    TRANSFORM tokenize
    SCOPE transaction
    PRIORITY 10;

-- Mask claim amount: show first 2 digits (preserves order of magnitude)
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.edi_claims (clm_2)
    TRANSFORM mask
    PARAMS (show = 2);

-- Hash patient names
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.edi_claims (nm1_3)
    TRANSFORM keyed_hash
    SCOPE person
    PARAMS (salt = 'edi_name_salt_2024');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.edi_claims (nm1_4)
    TRANSFORM keyed_hash
    SCOPE person
    PARAMS (salt = 'edi_name_salt_2024');

-- Generalize date of birth to decade
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.edi_claims (dmg_1)
    TRANSFORM generalize
    SCOPE relationship
    PARAMS (range = 10000);
