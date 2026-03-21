-- ============================================================================
-- Delta Column Mapping — Educational Queries
-- ============================================================================
-- WHAT: Column mapping decouples logical column names from physical Parquet
--       column names, enabling schema evolution operations like rename and drop
-- WHY:  Without column mapping, renaming a column requires rewriting every
--       Parquet file. With mapping mode 'name', only the metadata changes
-- HOW:  The Delta protocol tracks a column ID and physical name in the schema
--       metadata. TBLPROPERTIES 'delta.columnMapping.mode' = 'name' enables
--       this, requiring minReaderVersion=2 and minWriterVersion=5. ALTER TABLE
--       ADD/RENAME/DROP COLUMN only updates the transaction log, not data files.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline State — 40 employees, 8 columns
-- ============================================================================
-- Before any modifications, let's see the department breakdown. The table
-- currently has 8 columns (id through is_active) and all 40 employees are
-- active.

ASSERT ROW_COUNT = 6
ASSERT VALUE headcount = 8 WHERE department = 'Engineering'
ASSERT VALUE headcount = 5 WHERE department = 'Marketing'
ASSERT VALUE avg_salary = 110000 WHERE department = 'Engineering'
ASSERT VALUE avg_salary = 86800 WHERE department = 'Marketing'
SELECT department,
       COUNT(*) AS headcount,
       ROUND(AVG(salary), 0) AS avg_salary
FROM {{zone_name}}.delta_demos.employee_directory
GROUP BY department
ORDER BY headcount DESC;


-- ============================================================================
-- STEP 1: ALTER TABLE ADD COLUMN — schema evolution via column mapping
-- ============================================================================
-- This is the key operation that column mapping enables. Adding a new column
-- to a column-mapped table:
--   1. Adds the new column definition to the schema in the transaction log
--   2. Assigns it a unique column ID and physical name
--   3. Does NOT rewrite any existing Parquet files
-- Existing data files simply lack this column, so reads return NULL for it.
-- Without column mapping mode 'name', this operation would be more restrictive.

ALTER TABLE {{zone_name}}.delta_demos.employee_directory ADD COLUMN location VARCHAR;


-- ============================================================================
-- OBSERVE: The new column exists but is NULL for all existing rows
-- ============================================================================
-- Since no Parquet files were rewritten, every pre-existing row has NULL for
-- 'location'. This is a major advantage of column mapping — instant schema
-- evolution with zero I/O cost on data files.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 10
SELECT id, full_name, department,
       location,
       CASE WHEN location IS NULL THEN 'Added after insert - NULL expected'
            ELSE 'Has value' END AS column_mapping_note
FROM {{zone_name}}.delta_demos.employee_directory
ORDER BY id
LIMIT 10;


-- ============================================================================
-- STEP 2: UPDATE — promote 5 employees (Senior/Lead titles)
-- ============================================================================
-- In a column-mapped table, UPDATEs create new data files with the modified
-- rows. The column mapping ensures columns are referenced by their physical
-- IDs rather than names, so the UPDATE works correctly even though the schema
-- has evolved (location column added above).

ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.employee_directory SET title = 'Senior Software Engineer' WHERE id = 1;
ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.employee_directory SET title = 'Senior DevOps Engineer'   WHERE id = 3;
ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.employee_directory SET title = 'Lead Marketing Analyst'   WHERE id = 11;
ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.employee_directory SET title = 'Senior Financial Analyst' WHERE id = 21;
ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.employee_directory SET title = 'Senior Operations Analyst' WHERE id = 26;


-- ============================================================================
-- OBSERVE: Promoted employees after UPDATE
-- ============================================================================
-- Let's confirm the promotions took effect. Note that the 'location' column
-- is still NULL for these rows — the UPDATE changed only 'title'.

ASSERT ROW_COUNT = 5
SELECT id, full_name, department, title, salary, location
FROM {{zone_name}}.delta_demos.employee_directory
WHERE title LIKE '%Senior%' OR title LIKE '%Lead%'
ORDER BY department, full_name;


-- ============================================================================
-- STEP 3: UPDATE — deactivate 3 employees (is_active = 0)
-- ============================================================================
-- Another DML operation on the column-mapped table. The physical column IDs
-- ensure that is_active is correctly identified regardless of any schema
-- evolution that has occurred.

ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.employee_directory SET is_active = 0 WHERE id = 10;
ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.employee_directory SET is_active = 0 WHERE id = 19;
ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.employee_directory SET is_active = 0 WHERE id = 29;


-- ============================================================================
-- OBSERVE: Deactivated employees (ids 10, 19, 29)
-- ============================================================================

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 3
SELECT id, full_name, department, title, is_active
FROM {{zone_name}}.delta_demos.employee_directory
WHERE is_active = 0
ORDER BY id;


-- ============================================================================
-- LEARN: Column Mapping vs Non-Mapped Tables
-- ============================================================================
-- In a non-mapped Delta table, column identity is determined by name and
-- position in the Parquet schema. This means:
--   - Renaming a column requires rewriting all data files
--   - Dropping a column requires rewriting all data files
--   - Adding a column at a specific position is complex
--
-- With column mapping mode 'name':
--   - Each column has a unique ID (delta.columnMapping.id)
--   - Physical Parquet names can differ from logical names
--   - Rename = update metadata only
--   - Drop = update metadata only (data remains but is ignored)
--
-- Let's look at salary distribution across departments for active employees.

ASSERT ROW_COUNT = 6
ASSERT VALUE avg_salary = 110000 WHERE department = 'Engineering'
ASSERT VALUE avg_salary = 91286 WHERE department = 'Finance'
ASSERT VALUE avg_salary = 77167 WHERE department = 'HR'
ASSERT VALUE employees = 8 WHERE department = 'Engineering'
ASSERT VALUE employees = 5 WHERE department = 'Operations'
SELECT department,
       ROUND(MIN(salary), 0) AS min_salary,
       ROUND(AVG(salary), 0) AS avg_salary,
       ROUND(MAX(salary), 0) AS max_salary,
       COUNT(*) AS employees
FROM {{zone_name}}.delta_demos.employee_directory
WHERE is_active = 1
GROUP BY department
ORDER BY avg_salary DESC;


-- ============================================================================
-- EXPLORE: Full Employee Directory
-- ============================================================================

ASSERT ROW_COUNT = 40
SELECT id, full_name, department, title, email, start_date, salary, is_active,
       CASE WHEN location IS NULL THEN '(NULL)' ELSE location END AS location
FROM {{zone_name}}.delta_demos.employee_directory
ORDER BY id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Verification of row counts, department distribution, promotions, and the
-- NULL pattern from the column-mapped ADD COLUMN operation.

-- Verify total row count
ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.delta_demos.employee_directory;

-- Verify department count
ASSERT VALUE department_count = 6
SELECT COUNT(DISTINCT department) AS department_count FROM {{zone_name}}.delta_demos.employee_directory;

-- Verify promoted titles count
ASSERT VALUE promoted_count = 5
SELECT COUNT(*) FILTER (WHERE title LIKE '%Senior%' OR title LIKE '%Lead%') AS promoted_count
FROM {{zone_name}}.delta_demos.employee_directory;

-- Verify inactive employee count
ASSERT VALUE inactive_count = 3
SELECT COUNT(*) FILTER (WHERE is_active = 0) AS inactive_count FROM {{zone_name}}.delta_demos.employee_directory;

-- Verify active employee count
ASSERT VALUE active_count = 37
SELECT COUNT(*) FILTER (WHERE is_active = 1) AS active_count FROM {{zone_name}}.delta_demos.employee_directory;

-- Verify Engineering department count
ASSERT VALUE engineering_count = 8
SELECT COUNT(*) AS engineering_count FROM {{zone_name}}.delta_demos.employee_directory WHERE department = 'Engineering';

-- Verify all locations are NULL (added via column mapping)
ASSERT VALUE location_null_count = 40
SELECT COUNT(*) AS location_null_count FROM {{zone_name}}.delta_demos.employee_directory WHERE location IS NULL;

-- Verify salary for employee id 1
ASSERT VALUE salary = 115000.00
SELECT salary FROM {{zone_name}}.delta_demos.employee_directory WHERE id = 1;
