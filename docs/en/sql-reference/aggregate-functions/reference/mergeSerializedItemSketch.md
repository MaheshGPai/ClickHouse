---
description: 'Merges multiple serialized Frequent Items (ItemsSketch) sketches into a single sketch'
slug: /sql-reference/aggregate-functions/reference/mergeSerializedItemSketch
title: 'mergeSerializedItemSketch'
doc_type: 'reference'
---

# mergeSerializedItemSketch

Merges multiple serialized Apache DataSketches Frequent Items sketches that were created by [serializedItemSketch](/docs/en/sql-reference/aggregate-functions/reference/serializeditemsketch) into a single unified sketch.

## Syntax

```sql
mergeSerializedItemSketch([base64_encoded])(sketch_column)
```

## Arguments

- `sketch_column` — Column containing serialized item sketches. Type: [String](/docs/en/sql-reference/data-types/string.md).

## Parameters (optional)

- `base64_encoded` — If set to `1`, the input may be base64 encoded (useful for external sources). Type: [Bool](/docs/en/sql-reference/data-types/boolean.md). Default: `0`.

## Returned Value

- Merged serialized item sketch. Type: [String](/docs/en/sql-reference/data-types/string.md).

## Examples

```sql
WITH sketches AS (
  SELECT serializedItemSketch(item) AS sk
  FROM events
  GROUP BY day
)
SELECT mergeSerializedItemSketch(sk) AS merged
FROM sketches;
```

## See Also

- [serializedItemSketch](/docs/en/sql-reference/aggregate-functions/reference/serializeditemsketch) — Create item sketches
- [estimateFromItemSketch](/docs/en/sql-reference/functions/estimatefromitemsketch) — Query an item’s estimated frequency
- [topItemsFromItemSketch](/docs/en/sql-reference/functions/topitemsfromitemsketch) — Get top N items from a sketch

