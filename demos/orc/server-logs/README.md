# ORC Server Logs — HTTP Access Analytics

## Overview

This demo uses 5 ORC files containing HTTP access logs from a web
application cluster (3 web servers + 2 API servers, 500 requests each,
2,500 total). The files showcase ORC's self-describing format with schema
evolution across server types.

## Data

| File | Server | Rows | Schema | Compression |
|------|--------|------|--------|-------------|
| `web-01_access.orc` | web-01 | 500 | v1 (11 fields) | NONE |
| `web-02_access.orc` | web-02 | 500 | v1 (11 fields) | NONE |
| `web-03_access.orc` | web-03 | 500 | v1 (11 fields) | NONE |
| `api-01_access.orc` | api-01 | 500 | v2 (13 fields) | NONE |
| `api-02_access.orc` | api-02 | 500 | v2 (13 fields) | NONE |

### Schema Versions

**v1** (web servers): `request_id`, `server_name`, `timestamp`, `method`,
`endpoint`, `status_code`, `response_time_ms`, `response_bytes`,
`user_agent`, `client_ip`, `is_authenticated`

**v2** (API servers): all v1 fields + `request_body_bytes`, `cache_hit`

When reading all 5 files together, the union schema merges both versions.
Rows from v1 files get `NULL` for `request_body_bytes` and `cache_hit`.

## Tables

| Table | Rows | Features |
|-------|------|----------|
| `all_requests` | 2,500 | Multi-file, schema evolution, file_metadata |
| `api01_only` | 500 | file_filter, v2 schema with all columns |
| `requests_sample` | 250 | max_rows (50 per file), data profiling |

## ORC Features Demonstrated

| Feature | How |
|---------|-----|
| **Self-describing schema** | ORC file footers provide field names and types |
| **Schema evolution** | v1→v2 adds `request_body_bytes` + `cache_hit`; NULL filling |
| **Multi-file reading** | 5 files merged into one table |
| **file_filter** | `api-01*` selects single server |
| **max_rows** | 50 rows per file for sampling |
| **file_metadata** | `df_file_name`, `df_row_number` system columns |

## Queries

11 queries with 10 automated PASS/FAIL checks covering:
- Total row count verification
- Schema evolution NULL filling (web servers lack body_bytes/cache_hit)
- File filter extraction (API server 01 only)
- Max rows sampling
- File metadata population
- Column count with union schema
- HTTP status code distribution analytics
- Top endpoints by request count and error rate
