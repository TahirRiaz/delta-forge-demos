-- ============================================================================
-- Library Catalog — Index Lifecycle, Staleness, and Rebuild
-- ============================================================================
-- WHAT: Walks the index status state machine — current → stale →
--       rebuilt → current — with assertions at each transition.
-- WHY:  Indexes are managed objects, not magic. Knowing how they go
--       stale and how they recover is what lets you operate them
--       safely. The headline guarantee: a stale index NEVER produces
--       wrong answers; readers fall back to ordinary file pruning.
-- HOW:  This demo's index was created with auto_update = false, so any
--       write to the parent leaves it stale until REBUILD INDEX runs.
-- ============================================================================


-- ============================================================================
-- BUILD: Create the Index — auto_update DISABLED on purpose
-- ============================================================================
-- The reading workload is heavy (lookup by isbn) but writes are
-- batched into a nightly vendor load. We want the writers to stay
-- cheap, so we disable auto-update and plan to REBUILD INDEX once
-- per night after the load finishes. This is the configuration that
-- exposes the staleness lifecycle the rest of this demo walks through.

CREATE INDEX idx_isbn
    ON TABLE {{zone_name}}.delta_demos.library_catalog (isbn)
    WITH (auto_update = false);


-- ============================================================================
-- EXPLORE: Initial Catalog
-- ============================================================================
-- 40 books across 4 branches and several genres.

ASSERT ROW_COUNT = 4
ASSERT VALUE book_count = 10 WHERE branch = 'central'
ASSERT VALUE book_count = 10 WHERE branch = 'east'
ASSERT VALUE book_count = 10 WHERE branch = 'west'
ASSERT VALUE book_count = 10 WHERE branch = 'south'
SELECT branch,
       COUNT(*)                       AS book_count,
       SUM(copies)                    AS total_copies,
       SUM(available)                 AS available_copies
FROM {{zone_name}}.delta_demos.library_catalog
GROUP BY branch
ORDER BY branch;


-- ============================================================================
-- LEARN: Index Just-Built — Status Should Be `current`
-- ============================================================================
-- Right after CREATE INDEX, the index version equals the table
-- version. Aware readers will use it for any predicate on isbn.

DESCRIBE INDEX idx_isbn ON TABLE {{zone_name}}.delta_demos.library_catalog;


-- ============================================================================
-- LEARN: Lookup While Index is Current
-- ============================================================================
-- This point lookup is what we built the index for: routes straight
-- to the slice carrying the matching isbn.

ASSERT ROW_COUNT = 1
ASSERT VALUE title = 'Citadel of Glass'
ASSERT VALUE author = 'Roderik Stam'
ASSERT VALUE branch = 'east'
ASSERT VALUE copies = 8
SELECT isbn, title, author, genre, publish_year, copies, available, branch
FROM {{zone_name}}.delta_demos.library_catalog
WHERE isbn = '978-0016';


-- ============================================================================
-- LEARN: A Write Happens — Nightly Acquisitions
-- ============================================================================
-- The vendor delivers 5 new books overnight. With auto_update = false,
-- the parent's version moves forward but the index's version doesn't.
-- The index is now stale.

INSERT INTO {{zone_name}}.delta_demos.library_catalog VALUES
    ('978-0041','Cinder Bay',           'Aurelio Cifuentes',  'fiction',  2024, 5, 5, 'central'),
    ('978-0042','The Velvet Equation',  'Emiko Tanizaki',     'science',  2024, 4, 4, 'east'),
    ('978-0043','Reed and Saltwater',   'Ferdia Mac Cana',    'fiction',  2024, 6, 6, 'west'),
    ('978-0044','Telegraph Towers',     'Konstantin Veres',   'history',  2024, 3, 3, 'south'),
    ('978-0045','The Persimmon Year',   'Naoko Hartmann',     'fiction',  2024, 7, 7, 'central');

ASSERT ROW_COUNT = 1
ASSERT VALUE total_books = 45
SELECT COUNT(*) AS total_books
FROM {{zone_name}}.delta_demos.library_catalog;


-- ============================================================================
-- LEARN: Lookup During Stale Window — Correct Answer, Slower Path
-- ============================================================================
-- The new book is findable. The stale index is silently ignored;
-- the engine falls back to ordinary file pruning. This is the
-- safety guarantee — wrong answers are never possible, only slower
-- ones.

ASSERT ROW_COUNT = 1
ASSERT VALUE title = 'Cinder Bay'
ASSERT VALUE branch = 'central'
ASSERT VALUE publish_year = 2024
SELECT isbn, title, branch, publish_year, copies
FROM {{zone_name}}.delta_demos.library_catalog
WHERE isbn = '978-0041';


-- ============================================================================
-- LEARN: REBUILD INDEX — Bring it Back to Current
-- ============================================================================
-- Operator runs REBUILD as the nightly job's last step. Index
-- regenerates from the current table state and matches the parent
-- version again. Subsequent reads use it.

REBUILD INDEX idx_isbn ON TABLE {{zone_name}}.delta_demos.library_catalog;

DESCRIBE INDEX idx_isbn ON TABLE {{zone_name}}.delta_demos.library_catalog;


-- ============================================================================
-- LEARN: Lookup After Rebuild
-- ============================================================================
-- Same point lookup as before — now back on the fast path.

ASSERT ROW_COUNT = 1
ASSERT VALUE title = 'The Persimmon Year'
ASSERT VALUE author = 'Naoko Hartmann'
ASSERT VALUE branch = 'central'
SELECT isbn, title, author, branch, copies
FROM {{zone_name}}.delta_demos.library_catalog
WHERE isbn = '978-0045';


-- ============================================================================
-- LEARN: Switching to auto_update — Skip Manual Rebuilds
-- ============================================================================
-- If the team decides nightly rebuilds are operationally annoying,
-- ALTER flips on auto_update. Going forward, writes maintain the
-- index as part of the same commit.

ALTER INDEX idx_isbn ON TABLE {{zone_name}}.delta_demos.library_catalog
    SET (auto_update = true);

INSERT INTO {{zone_name}}.delta_demos.library_catalog VALUES
    ('978-0046','Quartet for Late Trains','Roselin Chambers','fiction',2024, 5, 5, 'east');

ASSERT ROW_COUNT = 1
ASSERT VALUE title = 'Quartet for Late Trains'
SELECT title FROM {{zone_name}}.delta_demos.library_catalog WHERE isbn = '978-0046';


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_books = 46
ASSERT VALUE distinct_isbn = 46
ASSERT VALUE distinct_branches = 4
ASSERT VALUE genre_count = 6
ASSERT VALUE total_copies = 236
SELECT COUNT(*)                         AS total_books,
       COUNT(DISTINCT isbn)             AS distinct_isbn,
       COUNT(DISTINCT branch)           AS distinct_branches,
       COUNT(DISTINCT genre)            AS genre_count,
       SUM(copies)                      AS total_copies
FROM {{zone_name}}.delta_demos.library_catalog;
