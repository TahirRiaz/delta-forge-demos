# Graph Social Network — 100-Node Company Graph

Demonstrates large-scale graph analytics on a 100-employee company social
network with 300+ directed connections, 8 departments, and 5 cities.

## Data Story

A company has 100 employees across Engineering, Marketing, HR, Finance, Sales,
Operations, Legal, and Product departments. Employees are distributed across
NYC, SF, Chicago, London, and Berlin. Three types of connections link them:
intra-department (colleagues/teammates), cross-department (projects/advisory),
and mentorship (senior to junior). All data is deterministically generated
via `generate_series()` for full reproducibility.

## Tables & Views

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `departments` | Delta Table | 8 | Department lookup (name, floor, budget) |
| `employees` | Delta Table | 100 | Employee vertex nodes (deterministic generation) |
| `connections` | Delta Table | 300+ | Directed edges with weight and type |
| `employee_stats` | View | 100 | Per-employee degree centrality metrics |
| `dept_connections` | View | ~64 | Cross-department connection matrix |

## Schema

**departments:** `dept_id INT, dept_name STRING, floor_num INT, budget_k INT`

**employees:** `id BIGINT, name STRING, age INT, department STRING, city STRING, title STRING, hire_year INT, level STRING, active BOOLEAN`

**connections:** `id BIGINT, src BIGINT, dst BIGINT, weight DOUBLE, relationship_type STRING, since_year INT`

**employee_stats (view):** employee columns + `out_degree, in_degree, total_degree`

**dept_connections (view):** `src_dept, dst_dept, connection_count, avg_weight`

## Data Generation

- **Employees:** `generate_series(1, 100)` with golden-ratio quasi-random age distribution
- **Intra-department edges:** ~100 same-department pairings (higher weight: 0.5-1.0)
- **Cross-department edges:** ~100 cross-department pairings (medium weight: 0.2-0.7)
- **Mentorship edges:** ~80 senior-to-junior pairings (high weight: 0.6-1.0)

## Graph Analytics Demonstrated

| Analysis | Query | Description |
|----------|-------|-------------|
| Degree centrality | #5 | In/out/total degree per employee |
| Department cross-pollination | #6 | Inter-department connection counts |
| Intra-department density | #7 | Within-department edge density |
| City network | #8 | Geographic distribution of connections |
| Influence scoring | #9 | In-degree as popularity/influence proxy |
| Isolated nodes | #10 | Employees with zero connections |
| 2-hop neighborhood | #11 | Reachability within 2 steps |
| Team clustering | #12 | Internal cohesion per department |
| Bridge detection | #13 | Employees connecting 3+ departments |
| Mentor network | #14 | Mentorship relationship analysis |
| Reciprocal connections | #15 | Bidirectional edges |

## Known Verification Values

| Check | Expected | Source |
|-------|----------|--------|
| Employee count | 100 | generate_series(1, 100) |
| Department count | 8 | Static insert |
| Connection count | >= 200 | Deterministic generation |
| City count | 5 | Modular assignment |
| Active employees | >= 80 | ~90% active rate |
| Weight range | 0.0–1.0 | Generation formula |
| No self-loops | 0 | WHERE src != dst filter |

## How to Verify

Run **Query #17 (Summary)** to see PASS/FAIL for all 10 checks. All should return `PASS`.

## What Makes This Demo Different

- **Larger scale** — 100 nodes + 300+ edges vs 5 nodes in mode demos
- **Deterministic generation** — `generate_series()` with golden-ratio quasi-random
- **Multiple relationship types** — colleague, teammate, collaborator, cross-team, project, social, advisory, mentor
- **Computed views** — degree centrality and cross-department metrics as lazy views
- **Graph analytics** — centrality, bridge detection, clustering, influence scoring
