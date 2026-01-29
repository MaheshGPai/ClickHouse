---
description: 'Extracts the estimated frequency (weight) of an item from a serialized ItemsSketch'
slug: /sql-reference/functions/estimateFromItemSketch
title: 'estimateFromItemSketch'
doc_type: 'reference'
---

# estimateFromItemSketch

Extracts the estimated frequency (weight) of a specific item from a serialized Frequent Items sketch (ItemsSketch) created by [serializedItemSketch](/docs/en/sql-reference/aggregate-functions/reference/serializeditemsketch) or [mergeSerializedItemSketch](/docs/en/sql-reference/aggregate-functions/reference/mergeserializeditemsketch).

## Syntax

```sql
estimateFromItemSketch(sketch, item)
```

## Arguments

- `sketch` — Serialized item sketch. Type: [String](/docs/en/sql-reference/data-types/string.md).
- `item` — Item to query. Type: [String](/docs/en/sql-reference/data-types/string.md).

## Returned Value

- Estimated frequency (weight). Type: [UInt64](/docs/en/sql-reference/data-types/int-uint.md).
- Returns 0 if the sketch is empty/invalid or the item is not tracked.

## Examples

```sql
WITH s AS (SELECT serializedItemSketch(concat('k', toString(number % 3))) AS sk FROM numbers(30))
SELECT
  estimateFromItemSketch((SELECT sk FROM s), 'k0') AS k0,
  estimateFromItemSketch((SELECT sk FROM s), 'k1') AS k1,
  estimateFromItemSketch((SELECT sk FROM s), 'k2') AS k2;
```

## See Also

- [serializedItemSketch](/docs/en/sql-reference/aggregate-functions/reference/serializeditemsketch)
- [mergeSerializedItemSketch](/docs/en/sql-reference/aggregate-functions/reference/mergeserializeditemsketch)
- [topItemsFromItemSketch](/docs/en/sql-reference/functions/topitemsfromitemsketch)

