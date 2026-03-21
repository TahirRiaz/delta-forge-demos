# Dolphins Social Network — Community Structure in the Wild

A well-studied animal social network. 62 bottlenose dolphins observed in Doubtful Sound, New Zealand, with 159 undirected association edges. The network naturally splits into 2–4 communities, making it a popular benchmark for community detection algorithms.

## Data Story

Between 1994 and 2001, researchers observed a community of 62 bottlenose dolphins in Doubtful Sound, New Zealand. They recorded which dolphins were seen together frequently, building a social network of associations. Analysis revealed the community had clear sub-group structure, with the network naturally dividing into distinct social clusters — a pattern common in animal social networks and useful for validating community detection algorithms.

## Data Source

D. Lusseau, K. Schneider, O. J. Boisseau, P. Haase, E. Slooten, and S. M. Dawson, "The bottlenose dolphin community of Doubtful Sound features a large proportion of long-lasting associations," Behavioral Ecology and Sociobiology, 54:396-405, 2003.

- Newman's network data: http://www-personal.umich.edu/~mejn/netdata/

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| vertices | Delta Table | 62 | Dolphins (IDs 0–61) |
| edges | Delta Table | 318 | Associations (159 undirected, stored bidirectionally) |

## Schema

**vertices:** `vertex_id BIGINT`

**edges:** `src BIGINT, dst BIGINT, weight DOUBLE`

## Graph Properties

- **Vertices:** 62 (bottlenose dolphins)
- **Edges:** 159 undirected (318 rows, bidirectional)
- **Weighted:** All weights = 1.0 (effectively unweighted)
- **Connected:** Yes (1 component)
- **Self-loops:** None

## Known Reference Values

| Metric | Expected Value | Source |
|--------|---------------|--------|
| Vertex count | 62 | Lusseau et al. (2003) |
| Edge count | 159 undirected | Lusseau et al. (2003) |
| Connected components | 1 | Fully connected |
| Communities | 2–4 (modularity ~0.49–0.53) | Published benchmarks |
| Max degree | 12 | Degree count |
| Triangle count | 285 | Published benchmarks |

## Algorithms Demonstrated

| Algorithm | Query | Description |
|-----------|-------|-------------|
| Degree distribution | #6 | Association count per dolphin |
| PageRank | #10 | Influence ranking |
| Degree centrality | #11 | Normalized degree |
| Betweenness centrality | #12 | Bridge node identification |
| Closeness centrality | #13 | Proximity to all others |
| Community detection | #14 | Recover natural groups |
| Connected components | #15 | Verify full connectivity |
| Shortest path | #16 | Distance between dolphins |

## How to Verify

Run **Query #17 (Verification Summary)** to see PASS/FAIL for all structural checks.
