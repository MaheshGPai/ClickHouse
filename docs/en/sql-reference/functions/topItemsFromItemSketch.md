---
description: 'Returns the top N items (by estimated frequency) from a serialized ItemsSketch'
slug: /sql-reference/functions/topItemsFromItemSketch
title: 'topItemsFromItemSketch'
doc_type: 'reference'
---

# topItemsFromItemSketch

Returns the top N items (by estimated frequency) tracked by a serialized Frequent Items sketch (ItemsSketch).

## Syntax

```sql
topItemsFromItemSketch(sketch, N)
```

## Arguments

- `sketch` — Serialized item sketch. Type: [String](/docs/en/sql-reference/data-types/string.md).
- `N` — Number of items to return. Type: integer. Must be > 0.

## Returned Value

- `Array(Tuple(String, UInt64, UInt64, UInt64))` — array of tuples:
  - `item`
  - `estimate`
  - `lower_bound`
  - `upper_bound`

The array is sorted by `estimate` descending. Empty/invalid sketches return an empty array.

## Examples

```sql
WITH s AS (SELECT serializedItemSketch(concat('k', toString(number % 3))) AS sk FROM numbers(30))
SELECT topItemsFromItemSketch((SELECT sk FROM s), 2);
```

## See Also

- [serializedItemSketch](/docs/en/sql-reference/aggregate-functions/reference/serializeditemsketch)
- [mergeSerializedItemSketch](/docs/en/sql-reference/aggregate-functions/reference/mergeserializeditemsketch)
- [estimateFromItemSketch](/docs/en/sql-reference/functions/estimatefromitemsketch)

