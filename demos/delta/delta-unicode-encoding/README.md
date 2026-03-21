# Delta Unicode & Encoding — International Data

Demonstrates Unicode and international character support in Delta tables
using a global product catalog with multi-script names.

## Data Story

A global marketplace sells products from 40+ countries. Each product has
an English name and a local-language name in its native script. Products
are partitioned by geographic region. Europe gets a 20% price increase,
and very cheap products are discontinued.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `global_products` | Delta Table | 48 (final) | International catalog partitioned by region |

## Schema

**global_products:** `id INT, product_name VARCHAR, product_name_local VARCHAR, category VARCHAR, price DOUBLE, currency VARCHAR, country VARCHAR, region VARCHAR` — PARTITIONED BY (region)

## Scripts Demonstrated

- **CJK:** Japanese (東京タワー), Chinese (北京烤鸭), Korean (김치)
- **Arabic:** Moroccan (زيت الأرغان), Lebanese (فلافل), Egyptian (ملاءة قطن)
- **Latin diacritics:** German (Würstchen), French (Crème brûlée), Spanish (Jalapeño)
- **Greek:** Ελληνικό Ελαιόλαδο
- **Czech:** Český Křišťál
- **Turkish:** İstanbul, Türk Lokumu

## Verification

8 automated PASS/FAIL checks verify row counts, region distribution, price
updates, deleted products, and Unicode string matching for Japanese, Arabic,
and German scripts.
