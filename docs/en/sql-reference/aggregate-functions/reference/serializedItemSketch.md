---
description: 'Creates a serialized Frequent Items (ItemsSketch) sketch for approximate item frequency estimation'
slug: /sql-reference/aggregate-functions/reference/serializedItemSketch
title: 'serializedItemSketch'
doc_type: 'reference'
---

# serializedItemSketch

Creates a serialized Apache DataSketches Frequent Items sketch (ItemsSketch) from string values, optionally with per-row weights. The sketch can be stored, transmitted, or merged with other sketches for distributed “top items” / approximate frequency analysis.

## Syntax

```sql
serializedItemSketch([lg_max_map_size])(item [, weight])
```

## Arguments

- `item` — Item value. Type: [String](/docs/en/sql-reference/data-types/string.md) or [FixedString](/docs/en/sql-reference/data-types/fixedstring.md).
- `weight` — Optional item weight (frequency increment). Type: integer. Default: 1.

## Parameters (optional)

- `lg_max_map_size` — Log2 of the maximum internal hash-map size used by the sketch. Type: integer. Default: 10.

## Returned Value

- Serialized binary item sketch. Type: [String](/docs/en/sql-reference/data-types/string.md).

## Examples

### Basic sketch

```sql
SELECT serializedItemSketch(concat('k', toString(number % 3))) AS sketch
FROM numbers(30);
```

### Weighted sketch

```sql
SELECT serializedItemSketch('a', 10) AS sketch;
```

## See Also

- [mergeSerializedItemSketch](/docs/en/sql-reference/aggregate-functions/reference/mergeserializeditemsketch) — Merge multiple item sketches
- [estimateFromItemSketch](/docs/en/sql-reference/functions/estimatefromitemsketch) — Query an item’s estimated frequency
- [topItemsFromItemSketch](/docs/en/sql-reference/functions/topitemsfromitemsketch) — Get top N items from a sketch

